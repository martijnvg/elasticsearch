/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsRequest;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ShardFollowTasksExecutor extends PersistentTasksExecutor<ShardFollowTask> {

    static final long DEFAULT_BATCH_SIZE = 1024;
    static final int PROCESSOR_RETRY_LIMIT = 16;
    static final int DEFAULT_CONCURRENT_PROCESSORS = 1;
    static final long DEFAULT_MAX_TRANSLOG_BYTES= Long.MAX_VALUE;
    private static final TimeValue CHECK_LEADER_GLOBAL_CHECKPOINT_INTERVAL = TimeValue.timeValueSeconds(3);

    private final Client client;
    private final ThreadPool threadPool;

    public ShardFollowTasksExecutor(Settings settings, Client client, ThreadPool threadPool) {
        super(settings, ShardFollowTask.NAME, Ccr.CCR_THREAD_POOL_NAME);
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    public void validate(ShardFollowTask params, ClusterState clusterState) {
        if (params.getLeaderClusterAlias() == null) {
            // We can only validate IndexRoutingTable in local cluster,
            // for remote cluster we would need to make a remote call and we cannot do this here.
            IndexRoutingTable routingTable = clusterState.getRoutingTable().index(params.getLeaderShardId().getIndex());
            if (routingTable.shard(params.getLeaderShardId().id()).primaryShard().started() == false) {
                throw new IllegalArgumentException("Not all copies of leader shard are started");
            }
        }

        IndexRoutingTable routingTable = clusterState.getRoutingTable().index(params.getFollowShardId().getIndex());
        if (routingTable.shard(params.getFollowShardId().id()).primaryShard().started() == false) {
            throw new IllegalArgumentException("Not all copies of follow shard are started");
        }
    }

    @Override
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                 PersistentTasksCustomMetaData.PersistentTask<ShardFollowTask> taskInProgress,
                                                 Map<String, String> headers) {
        return new ShardFollowNodeTask(id, type, action, getDescription(taskInProgress), parentTaskId, headers);
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, ShardFollowTask params, Task.Status status) {
        ShardFollowNodeTask shardFollowNodeTask = (ShardFollowNodeTask) task;
        Client leaderClient = wrapClient(params.getLeaderClusterAlias() != null ?
                this.client.getRemoteClusterClient(params.getLeaderClusterAlias()) : this.client, params);
        Client followerClient = wrapClient(this.client, params);
        IndexMetadataVersionChecker imdVersionChecker = new IndexMetadataVersionChecker(params.getLeaderShardId().getIndex(),
                params.getFollowShardId().getIndex(), client, leaderClient);
        logger.info("[{}] initial leader mapping with follower mapping syncing", params);
        imdVersionChecker.updateMapping(1L /* Force update, version is initially 0L */, e -> {
            if (e == null) {
                logger.info("Starting shard following [{}]", params);
                fetchGlobalCheckpoint(followerClient, params.getFollowShardId(),
                        followGlobalCheckPoint -> {
                            shardFollowNodeTask.updateProcessedGlobalCheckpoint(followGlobalCheckPoint);
                            prepare(leaderClient, followerClient,shardFollowNodeTask, params, followGlobalCheckPoint, imdVersionChecker);
                        }, task::markAsFailed);
            } else {
                shardFollowNodeTask.markAsFailed(e);
            }
        });
    }

    void prepare(Client leaderClient, Client followerClient, ShardFollowNodeTask task, ShardFollowTask params,
                 long followGlobalCheckPoint,
                 IndexMetadataVersionChecker imdVersionChecker) {

        final ShardId leaderShard = params.getLeaderShardId();
        final ShardId followerShard = params.getFollowShardId();
        fetchGlobalCheckpoint(leaderClient, leaderShard, leaderGlobalCheckPoint -> {
            logger.debug("{} fetching write operations, leaderGlobalCheckPoint={}, followGlobalCheckPoint={}", followerShard,
                leaderGlobalCheckPoint, followGlobalCheckPoint);
            ChunksCoordinator coordinator = new ChunksCoordinator(followerClient, leaderClient, threadPool, imdVersionChecker, params.getMaxChunkSize(),
                params.getNumConcurrentChunks(), params.getProcessorMaxTranslogBytes(), leaderShard, followerShard, task::markAsFailed,
                task::isRunning, task::updateProcessedGlobalCheckpoint);
            coordinator.start(followGlobalCheckPoint, leaderGlobalCheckPoint);
        }, task::markAsFailed);
    }

    private static void fetchGlobalCheckpoint(Client client, ShardId shardId, LongConsumer handler, Consumer<Exception> errorHandler) {
        client.admin().indices().stats(new IndicesStatsRequest().indices(shardId.getIndexName()), ActionListener.wrap(r -> {
            IndexStats indexStats = r.getIndex(shardId.getIndexName());
            Optional<ShardStats> filteredShardStats = Arrays.stream(indexStats.getShards())
                    .filter(shardStats -> shardStats.getShardRouting().shardId().equals(shardId))
                    .filter(shardStats -> shardStats.getShardRouting().primary())
                    .findAny();

            if (filteredShardStats.isPresent()) {
                final long globalCheckPoint = filteredShardStats.get().getSeqNoStats().getGlobalCheckpoint();
                handler.accept(globalCheckPoint);
            } else {
                errorHandler.accept(new IllegalArgumentException("Cannot find shard stats for shard " + shardId));
            }
        }, errorHandler));
    }

    static class ChunksCoordinator {

        private static final Logger LOGGER = Loggers.getLogger(ChunksCoordinator.class);
    
        private final Client followerClient;
        private final Client leaderClient;
        private final ThreadPool threadPool;
        private final IndexMetadataVersionChecker imdVersionChecker;

        private final long batchSize;
        private final int maxConcurrentWorker;
        private final long processorMaxTranslogBytes;
        private final ShardId leaderShard;
        private final ShardId followerShard;
        private final Consumer<Exception> failureHandler;
        private final Supplier<Boolean> stateSupplier;
        private final LongConsumer processedGlobalCheckpointUpdater;
    
        private final AtomicInteger activeWorkers;
        private final AtomicLong lastPolledGlobalCheckpoint;
        private final AtomicLong lastProcessedGlobalCheckPoint;
        private final Queue<long[]> chunks = new ConcurrentLinkedQueue<>();

        ChunksCoordinator(Client followerClient,
                          Client leaderClient,
                          ThreadPool threadPool,
                          IndexMetadataVersionChecker imdVersionChecker,
                          long batchSize,
                          int maxConcurrentWorker,
                          long processorMaxTranslogBytes,
                          ShardId leaderShard,
                          ShardId followerShard,
                          Consumer<Exception> failureHandler,
                          Supplier<Boolean> runningSuppler,
                          LongConsumer processedGlobalCheckpointUpdater) {
            this.followerClient = followerClient;
            this.leaderClient = leaderClient;
            this.threadPool = threadPool;
            this.imdVersionChecker = imdVersionChecker;
            this.batchSize = batchSize;
            this.maxConcurrentWorker = maxConcurrentWorker;
            this.processorMaxTranslogBytes = processorMaxTranslogBytes;
            this.leaderShard = leaderShard;
            this.followerShard = followerShard;
            this.failureHandler = failureHandler;
            this.stateSupplier = runningSuppler;
            this.processedGlobalCheckpointUpdater = processedGlobalCheckpointUpdater;
            this.activeWorkers = new AtomicInteger();
            this.lastPolledGlobalCheckpoint = new AtomicLong();
            this.lastProcessedGlobalCheckPoint = new AtomicLong();
        }

        void createChucks(long from, long to) {
            LOGGER.debug("{} Creating chunks for operation range [{}] to [{}]", leaderShard, from, to);
            for (long i = from; i < to; i += batchSize) {
                long v2 = i + batchSize < to ? i + batchSize : to;
                chunks.add(new long[]{i == from ? i : i + 1, v2});
            }
        }

        void update() {
            schedule(() -> {
                if (stateSupplier.get() == false) {
                    return;
                }
                
                fetchGlobalCheckpoint(leaderClient, leaderShard, leaderGlobalCheckPoint -> {
                    long followerGlobalCheckpoint = lastPolledGlobalCheckpoint.get();
                    if (leaderGlobalCheckPoint != followerGlobalCheckpoint) {
                        assert followerGlobalCheckpoint < leaderGlobalCheckPoint : "followGlobalCheckPoint [" + followerGlobalCheckpoint +
                            "] is not below leaderGlobalCheckPoint [" + leaderGlobalCheckPoint + "]";
                        createChucks(lastPolledGlobalCheckpoint.get(), leaderGlobalCheckPoint);
                        initiateChunkWorkers();
                    } else {
                        LOGGER.debug("{} no write operations to fetch", followerShard);
                    }
                    update();
                }, failureHandler);
            });
        }
        
        void start(long followerGlobalCheckpoint, long leaderGlobalCheckPoint) {
            createChucks(followerGlobalCheckpoint, leaderGlobalCheckPoint);
            lastPolledGlobalCheckpoint.set(leaderGlobalCheckPoint);
            LOGGER.debug("{} Start coordination of [{}] chunks with [{}] concurrent processors",
                    leaderShard, chunks.size(), maxConcurrentWorker);
            initiateChunkWorkers();
            update();
        }
        
        void initiateChunkWorkers() {
            int workersToStart = maxConcurrentWorker - activeWorkers.get();
            if (workersToStart == 0) {
                LOGGER.debug("{} No new chunk workers were started", followerShard);
                return;
            }
            
            LOGGER.debug("{} Starting [{}] new chunk workers", followerShard, workersToStart);
            for (int i = 0; i < workersToStart; i++) {
                threadPool.executor(Ccr.CCR_THREAD_POOL_NAME).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        assert e != null;
                        LOGGER.error(() -> new ParameterizedMessage("{} Failure starting processor", followerShard), e);
                        failureHandler.accept(e);
                    }
            
                    @Override
                    protected void doRun() throws Exception {
                        processNextChunk();
                    }
                });
                activeWorkers.incrementAndGet();
            }
        }

        void processNextChunk() {
            if (stateSupplier.get() == false) {
                return;
            }
            
            long[] chunk = chunks.poll();
            if (chunk == null) {
                int activeWorkers = this.activeWorkers.decrementAndGet();
                LOGGER.debug("{} No more chunks to process, active workers [{}]", leaderShard, activeWorkers);
                return;
            }
            LOGGER.debug("{} Processing chunk [{}/{}]", leaderShard, chunk[0], chunk[1]);
            Consumer<Exception> processorHandler = e -> {
                if (e == null) {
                    LOGGER.debug("{} Successfully processed chunk [{}/{}]", leaderShard, chunk[0], chunk[1]);
                    if (lastPolledGlobalCheckpoint.updateAndGet(x -> x < chunk[1] ? chunk[1] : x) == chunk[1]) {
                        processedGlobalCheckpointUpdater.accept(chunk[1]);
                    }
                    processNextChunk();
                } else {
                    LOGGER.error(() -> new ParameterizedMessage("{} Failure processing chunk [{}/{}]",
                            leaderShard, chunk[0], chunk[1]), e);
                    failureHandler.accept(e);
                }
            };
            Executor ccrExecutor = threadPool.executor(Ccr.CCR_THREAD_POOL_NAME);
            ChunkProcessor processor = new ChunkProcessor(leaderClient, followerClient, chunks, ccrExecutor, imdVersionChecker,
                    leaderShard, followerShard, processorHandler);
            processor.start(chunk[0], chunk[1], processorMaxTranslogBytes);
        }

        void schedule(Runnable runnable) {
            threadPool.schedule(CHECK_LEADER_GLOBAL_CHECKPOINT_INTERVAL, Ccr.CCR_THREAD_POOL_NAME, new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    failureHandler.accept(e);
                }
            
                @Override
                protected void doRun() throws Exception {
                    runnable.run();
                }
            });
        }

        Queue<long[]> getChunks() {
            return chunks;
        }

    }

    static class ChunkProcessor {

        private final Client leaderClient;
        private final Client followerClient;
        private final Queue<long[]> chunks;
        private final Executor ccrExecutor;
        private final BiConsumer<Long, Consumer<Exception>> indexVersionChecker;

        private final ShardId leaderShard;
        private final ShardId followerShard;
        private final Consumer<Exception> handler;
        final AtomicInteger retryCounter = new AtomicInteger(0);

        ChunkProcessor(Client leaderClient, Client followerClient, Queue<long[]> chunks, Executor ccrExecutor,
                       BiConsumer<Long, Consumer<Exception>> indexVersionChecker,
                       ShardId leaderShard, ShardId followerShard, Consumer<Exception> handler) {
            this.leaderClient = leaderClient;
            this.followerClient = followerClient;
            this.chunks = chunks;
            this.ccrExecutor = ccrExecutor;
            this.indexVersionChecker = indexVersionChecker;
            this.leaderShard = leaderShard;
            this.followerShard = followerShard;
            this.handler = handler;
        }

        void start(final long from, final long to, final long maxTranslogsBytes) {
            ShardChangesAction.Request request = new ShardChangesAction.Request(leaderShard);
            // Treat -1 as 0, because shard changes api min_seq_no is inclusive and therefore it doesn't allow a negative min_seq_no
            // (If no indexing has happened in leader shard then global checkpoint is -1.)
            request.setMinSeqNo(Math.max(0, from));
            request.setMaxSeqNo(to);
            request.setMaxTranslogsBytes(maxTranslogsBytes);
            leaderClient.execute(ShardChangesAction.INSTANCE, request, new ActionListener<ShardChangesAction.Response>() {
                @Override
                public void onResponse(ShardChangesAction.Response response) {
                    handleResponse(to, response);
                }

                @Override
                public void onFailure(Exception e) {
                    assert e != null;
                    if (shouldRetry(e)) {
                        if (retryCounter.incrementAndGet() <= PROCESSOR_RETRY_LIMIT) {
                            start(from, to, maxTranslogsBytes);
                        } else {
                            handler.accept(new ElasticsearchException("retrying failed [" + retryCounter.get() +
                                    "] times, aborting...", e));
                        }
                    } else {
                        handler.accept(e);
                    }
                }
            });
        }

        void handleResponse(final long to, final ShardChangesAction.Response response) {
            if (response.getOperations().length != 0) {
                Translog.Operation lastOp = response.getOperations()[response.getOperations().length - 1];
                boolean maxByteLimitReached = lastOp.seqNo() < to;
                if (maxByteLimitReached) {
                    // add a new entry to the queue for the operations that couldn't be fetched in the current shard changes api call:
                    chunks.add(new long[]{lastOp.seqNo() + 1, to});
                }
            }
            ccrExecutor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    assert e != null;
                    handler.accept(e);
                }

                @Override
                protected void doRun() throws Exception {
                    indexVersionChecker.accept(response.getIndexMetadataVersion(), e -> {
                        if (e != null) {
                            if (shouldRetry(e) && retryCounter.incrementAndGet() <= PROCESSOR_RETRY_LIMIT) {
                                handleResponse(to, response);
                            } else {
                                handler.accept(new ElasticsearchException("retrying failed [" + retryCounter.get() +
                                        "] times, aborting...", e));
                            }
                            return;
                        }
                        final BulkShardOperationsRequest request = new BulkShardOperationsRequest(followerShard, response.getOperations());
                        followerClient.execute(BulkShardOperationsAction.INSTANCE, request,
                            new ActionListener<BulkShardOperationsResponse>() {
                            @Override
                            public void onResponse(final BulkShardOperationsResponse bulkShardOperationsResponse) {
                                handler.accept(null);
                            }

                                @Override
                                public void onFailure(final Exception e) {
                                    // No retry mechanism here, because if a failure is being redirected to this place it is considered
                                    // non recoverable.
                                    assert e != null;
                                    handler.accept(e);
                                }
                            }
                        );
                    });
                }
            });
        }

        boolean shouldRetry(Exception e) {
            // TODO: What other exceptions should be retried?
            return NetworkExceptionHelper.isConnectException(e) ||
                    NetworkExceptionHelper.isCloseConnectionException(e);
        }

    }

    static Client wrapClient(Client client, ShardFollowTask shardFollowTask) {
        if (shardFollowTask.getHeaders().isEmpty()) {
            return client;
        } else {
            final ThreadContext threadContext = client.threadPool().getThreadContext();
            Map<String, String> filteredHeaders = shardFollowTask.getHeaders().entrySet().stream()
                .filter(e -> ShardFollowTask.HEADER_FILTERS.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return new FilterClient(client) {
                @Override
                protected <
                    Request extends ActionRequest,
                    Response extends ActionResponse,
                    RequestBuilder extends ActionRequestBuilder<Request, Response>>
                void doExecute(Action<Request, Response> action, Request request, ActionListener<Response> listener) {
                    final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
                    try (ThreadContext.StoredContext ignore = stashWithHeaders(threadContext, filteredHeaders)) {
                        super.doExecute(action, request, new ContextPreservingActionListener<>(supplier, listener));
                    }
                }
            };
        }
    }

    private static ThreadContext.StoredContext stashWithHeaders(ThreadContext threadContext, Map<String, String> headers) {
        final ThreadContext.StoredContext storedContext = threadContext.stashContext();
        threadContext.copyHeaders(headers.entrySet());
        return storedContext;
    }

    static final class IndexMetadataVersionChecker implements BiConsumer<Long, Consumer<Exception>> {

        private static final Logger LOGGER = Loggers.getLogger(IndexMetadataVersionChecker.class);

        private final Client followClient;
        private final Client leaderClient;
        private final Index leaderIndex;
        private final Index followIndex;
        private final AtomicLong currentIndexMetadataVersion;
        private final Semaphore updateMappingSemaphore = new Semaphore(1);

        IndexMetadataVersionChecker(Index leaderIndex, Index followIndex, Client followClient, Client leaderClient) {
            this.followClient = followClient;
            this.leaderIndex = leaderIndex;
            this.followIndex = followIndex;
            this.leaderClient = leaderClient;
            this.currentIndexMetadataVersion = new AtomicLong();
        }

        public void accept(Long minimumRequiredIndexMetadataVersion, Consumer<Exception> handler) {
            if (currentIndexMetadataVersion.get() >= minimumRequiredIndexMetadataVersion) {
                LOGGER.debug("Current index metadata version [{}] is higher or equal than minimum required index metadata version [{}]",
                    currentIndexMetadataVersion.get(), minimumRequiredIndexMetadataVersion);
                handler.accept(null);
            } else {
                updateMapping(minimumRequiredIndexMetadataVersion, handler);
            }
        }

        void updateMapping(long minimumRequiredIndexMetadataVersion, Consumer<Exception> handler) {
            try {
                updateMappingSemaphore.acquire();
            } catch (InterruptedException e) {
                handler.accept(e);
                return;
            }
            if (currentIndexMetadataVersion.get() >= minimumRequiredIndexMetadataVersion) {
                updateMappingSemaphore.release();
                LOGGER.debug("Current index metadata version [{}] is higher or equal than minimum required index metadata version [{}]",
                    currentIndexMetadataVersion.get(), minimumRequiredIndexMetadataVersion);
                handler.accept(null);
                return;
            }

            ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
            clusterStateRequest.clear();
            clusterStateRequest.metaData(true);
            clusterStateRequest.indices(leaderIndex.getName());

            leaderClient.admin().cluster().state(clusterStateRequest, ActionListener.wrap(clusterStateResponse -> {
                IndexMetaData indexMetaData = clusterStateResponse.getState().metaData().getIndexSafe(leaderIndex);
                assert indexMetaData.getMappings().size() == 1;
                MappingMetaData mappingMetaData = indexMetaData.getMappings().iterator().next().value;

                PutMappingRequest putMappingRequest = new PutMappingRequest(followIndex.getName());
                putMappingRequest.type(mappingMetaData.type());
                putMappingRequest.source(mappingMetaData.source().string(), XContentType.JSON);
                followClient.admin().indices().putMapping(putMappingRequest, ActionListener.wrap(putMappingResponse -> {
                    currentIndexMetadataVersion.set(indexMetaData.getVersion());
                    updateMappingSemaphore.release();
                    handler.accept(null);
                }, e -> {
                    updateMappingSemaphore.release();
                    handler.accept(e);
                }));
            }, e -> {
                updateMappingSemaphore.release();
                handler.accept(e);
            }));
        }
    }

}
