/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionTransportException;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsRequest;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ShardFollowTasksExecutor extends PersistentTasksExecutor<ShardFollowTask> {

    static final long DEFAULT_MAX_READ_SIZE = 1024;
    static final int PROCESSOR_RETRY_LIMIT = 5;
    static final int DEFAULT_MAX_CONCURRENT_READS = 5;
    static final long DEFAULT_MAX_TRANSLOG_BYTES= Long.MAX_VALUE;
    private static final TimeValue RETRY_TIMEOUT = TimeValue.timeValueMillis(500);

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
    protected void nodeOperation(final AllocatedPersistentTask task, final ShardFollowTask params, final PersistentTaskState state) {
        ShardFollowNodeTask shardFollowNodeTask = (ShardFollowNodeTask) task;
        logger.info("[{}] Starting to track leader shard [{}], params [{}]", params.getFollowShardId(), params.getLeaderShardId(), params);
        FollowShardTracker tracker = createFollowShardTracker(client, threadPool, params, shardFollowNodeTask);
        tracker.versionChecker.accept(1L /* Force update, version is initially 0L */, e -> {
            if (e == null) {
                fetchGlobalCheckpoint(tracker.followerClient, params.getFollowShardId(), followGlobalCheckPoint -> {
                    shardFollowNodeTask.updateProcessedGlobalCheckpoint(followGlobalCheckPoint);
                    tracker.start(followGlobalCheckPoint);
                    logger.info("[{}] Started to track leader shard [{}]", params.getFollowShardId(), params.getLeaderShardId());
                }, task::markAsFailed);
            } else {
                shardFollowNodeTask.markAsFailed(e);
            }
        });
    }

    private void fetchGlobalCheckpoint(Client client, ShardId shardId, LongConsumer handler, Consumer<Exception> errorHandler) {
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

    private static FollowShardTracker createFollowShardTracker(Client client, ThreadPool threadPool,
                                                               ShardFollowTask params, ShardFollowNodeTask shardFollowNodeTask) {

        Client leaderClient = wrapClient(params.getLeaderClusterAlias() != null ?
            client.getRemoteClusterClient(params.getLeaderClusterAlias()) : client, params);
        Client followerClient = wrapClient(client, params);

        IndexMetadataVersionChecker imdVersionChecker = new IndexMetadataVersionChecker(params.getLeaderShardId().getIndex(),
            params.getFollowShardId().getIndex(), client, leaderClient);

        BiFunction<TimeValue, Runnable, ScheduledFuture<?>> scheduler =
            (delay, command) -> threadPool.schedule(delay, Ccr.CCR_THREAD_POOL_NAME, command);

        return new FollowShardTracker(leaderClient, followerClient, scheduler, imdVersionChecker, params, shardFollowNodeTask);
    }

    static class FollowShardTracker {

        private static final Logger LOGGER = Loggers.getLogger(FollowShardTracker.class);

        private final Client leaderClient;
        private final Client followerClient;
        private final BiFunction<TimeValue, Runnable, ScheduledFuture<?>> scheduler;
        private final BiConsumer<Long, Consumer<Exception>> versionChecker;

        private final ShardFollowTask params;
        private final ShardFollowNodeTask nodeTask;

        private volatile long lastSeenSeqNo = -1;
//        private volatile long lastProcessedSeqNo = -1;
        private final Set<Object> ongoingReads = new HashSet<>();
        private final Queue<Translog.Operation> writeQueue = new LinkedList<>();

        FollowShardTracker(Client leaderClient, Client followerClient, BiFunction<TimeValue, Runnable, ScheduledFuture<?>> scheduler,
                           BiConsumer<Long, Consumer<Exception>> versionChecker, ShardFollowTask params, ShardFollowNodeTask nodeTask) {
            this.leaderClient = leaderClient;
            this.followerClient = followerClient;
            this.scheduler = scheduler;
            this.versionChecker = versionChecker;
            this.params = params;
            this.nodeTask = nodeTask;
        }

        synchronized void start(long followGlobalCheckPoint) {
            Object token = newToken();
            LOGGER.info("{}[{}] starting", params.getFollowShardId(), token);
            getWriteOperations(token, followGlobalCheckPoint, response -> handleShardChangesResponse(token, response));
            scheduler.apply(TimeValue.timeValueMillis(500), this::schedule);
        }

        private void schedule() {
            if (nodeTask.isRunning()) {
                if (tokenCounter.get() == 0) {
                    synchronized (this) {
                        Object token = newToken();
                        LOGGER.info("{}[{}] re-starting", params.getFollowShardId(), token);
                        getWriteOperations(token, lastSeenSeqNo, response -> handleShardChangesResponse(token, response));
                    }
                } else {
                    LOGGER.info("{} not restarting", params.getFollowShardId());
                }
                scheduler.apply(TimeValue.timeValueMillis(500), this::schedule);
            } else {
                LOGGER.info("{} shard follow task has been stopped", params.getFollowShardId());
            }
        }

        synchronized void coordinate(Object token, long leaderGlobalCheckpoint) {
            assert token != null;
            if (nodeTask.isRunning() == false || lastSeenSeqNo >= leaderGlobalCheckpoint) {
                deleteToken(token);
                return;
            }

            LOGGER.info("{}[{}] coordinate [{}][{}]", params.getFollowShardId(), token, lastSeenSeqNo, leaderGlobalCheckpoint);
            long from = lastSeenSeqNo + 1;
            getWriteOperations(token, from, response -> handleShardChangesResponse(token, response));

            from += params.getMaxReadSize();
            while (from <= leaderGlobalCheckpoint && ongoingReads.size() <= params.getNumConcurrentReads()) {
                Object newToken = newToken();
                LOGGER.info("{}[{}] coordinate [{}][{}]", params.getFollowShardId(), token, lastSeenSeqNo, leaderGlobalCheckpoint);
                getWriteOperations(token, from, response -> handleShardChangesResponse(newToken, response));
                from += params.getMaxReadSize();
            }
        }

        synchronized void handleShardChangesResponse(Object token, ShardChangesAction.Response response) {
            versionChecker.accept(response.getIndexMetadataVersion(), e -> {
                synchronized (this) {
                    if (e != null) {
                        handleFailure(e, () -> handleShardChangesResponse(token, response));
                    } else {
                        if (response.getOperations().length != 0) {
                            LOGGER.info("{}[{}] received [{}] new ops, leader global checkpoint [{}]",
                                params.getFollowShardId(), token, response.getOperations().length, response.getLeaderGlobalCheckpoint());
                            writeQueue.addAll(Arrays.asList(response.getOperations()));
                            lastSeenSeqNo = Math.max(lastSeenSeqNo, response.getHighestSeqNo());
                            maybeFlushWriteQueue(token);
                            coordinate(token, response.getLeaderGlobalCheckpoint());
                        } else {
                            LOGGER.info("{}[{}] received no new ops, leader global checkpoint [{}]",
                                params.getFollowShardId(), token, response.getLeaderGlobalCheckpoint());
                            maybeFlushWriteQueue(token);
                            coordinate(token, response.getLeaderGlobalCheckpoint());
                        }
                    }
                }
            });
        }

        synchronized void maybeFlushWriteQueue(Object token) {
            boolean shouldFlush = writeQueue.size() > 1000;
            if (shouldFlush == false) {
                shouldFlush = writeQueue.stream().mapToLong(Translog.Operation::estimateSize).sum() > ByteSizeUnit.MB.toBytes(1);
            }
            if (shouldFlush) {
                LOGGER.info("{}[{}] Flush threshold met", params.getFollowShardId(), token);
                flushWriteQueue(token);
            }
        }

        private void flushWriteQueue(Object token) {
            assert Thread.holdsLock(this);
            LOGGER.info("{}[{}]  Flushing [{}] write operations", params.getFollowShardId(), token, writeQueue.size());
            Translog.Operation[] ops = writeQueue.toArray(new Translog.Operation[0]);
            writeQueue.clear();
            persistWriteOperations(token, ops, bulkShardOperationsResponse -> {
                nodeTask.updateProcessedGlobalCheckpoint(lastSeenSeqNo);
                maybeFlushWriteQueue(token);
            });
        }

        private void getWriteOperations(Object token, long fromSeqNo, Consumer<ShardChangesAction.Response> responseHandler) {
            assert Thread.holdsLock(this);
            ShardChangesAction.Request request = new ShardChangesAction.Request(params.getLeaderShardId());
            request.setFromSeqNo(Math.max(0, fromSeqNo));
            request.setSize(params.getMaxReadSize());
            LOGGER.info("{}[{}] getWriteOperations [{}][{}]", params.getFollowShardId(), token, request.getFromSeqNo(), request.getSize());
            leaderClient.execute(ShardChangesAction.INSTANCE, request, new ActionListener<ShardChangesAction.Response>() {
                @Override
                public void onResponse(ShardChangesAction.Response response) {
                   responseHandler.accept(response);
                }

                @Override
                public void onFailure(Exception e) {
                    handleFailure(e, () -> getWriteOperations(token, fromSeqNo, responseHandler));
                }
            });
        }

        private void persistWriteOperations(Object token, Translog.Operation[] operations,
                                            Consumer<BulkShardOperationsResponse> responseHandler) {
            assert Thread.holdsLock(this);
            final BulkShardOperationsRequest request = new BulkShardOperationsRequest(params.getFollowShardId(), operations);
            followerClient.execute(BulkShardOperationsAction.INSTANCE, request,
                new ActionListener<BulkShardOperationsResponse>() {
                    @Override
                    public void onResponse(BulkShardOperationsResponse response) {
                        responseHandler.accept(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        handleFailure(e, () -> persistWriteOperations(token, operations, responseHandler));
                    }
                }
            );
        }

        private final AtomicInteger tokenCounter = new AtomicInteger(0);

        private Object newToken() {
            assert Thread.holdsLock(this);
            Object token = tokenCounter.getAndIncrement();
            boolean added = ongoingReads.add(token);
            assert added;
            return token;
        }

        private void deleteToken(Object token) {
            assert Thread.holdsLock(this);
            boolean removed = ongoingReads.remove(token);
            assert removed;
            int result = tokenCounter.decrementAndGet();
            assert result >= 0;
            if (result == 0 && writeQueue.isEmpty() == false) {
                LOGGER.info("{}[{}] Force flush", params.getFollowShardId(), token);
                flushWriteQueue(token);
            }
        }

        private final AtomicInteger retryCounter = new AtomicInteger(0);

        private void handleFailure(Exception e, Runnable task) {
            assert e != null;
            if (shouldRetry(e)) {
                if (nodeTask.isRunning() && retryCounter.incrementAndGet() <= PROCESSOR_RETRY_LIMIT) {
                    scheduler.apply(RETRY_TIMEOUT, () -> {
                        synchronized (this) {
                            task.run();
                        }
                    });
                } else {
                    nodeTask.markAsFailed(new ElasticsearchException("retrying failed [" + retryCounter.get() +
                        "] times, aborting...", e));
                }
            } else {
                nodeTask.markAsFailed(e);
            }
        }

        private boolean shouldRetry(Exception e) {
            // TODO: What other exceptions should be retried?
            return NetworkExceptionHelper.isConnectException(e) ||
                NetworkExceptionHelper.isCloseConnectionException(e) ||
                e instanceof ActionTransportException ||
                e instanceof NodeClosedException ||
                e instanceof UnavailableShardsException ||
                e instanceof NoShardAvailableActionException;
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
                void doExecute(Action<Response> action, Request request, ActionListener<Response> listener) {
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

        IndexMetadataVersionChecker(Index leaderIndex, Index followIndex, Client followClient, Client leaderClient) {
            this.followClient = followClient;
            this.leaderIndex = leaderIndex;
            this.followIndex = followIndex;
            this.leaderClient = leaderClient;
            this.currentIndexMetadataVersion = new AtomicLong();
        }

        public void accept(Long minimumRequiredIndexMetadataVersion, Consumer<Exception> handler) {
            if (currentIndexMetadataVersion.get() >= minimumRequiredIndexMetadataVersion) {
                LOGGER.trace("current index metadata version [{}] >= minimum required index metadata version [{}]",
                    currentIndexMetadataVersion.get(), minimumRequiredIndexMetadataVersion);
                handler.accept(null);
            } else {
                LOGGER.debug("updating mapping, current index metadata version [{}] < required minimum index metadata version [{}]",
                    currentIndexMetadataVersion.get(), minimumRequiredIndexMetadataVersion);
                updateMapping(handler);
            }
        }

        void updateMapping(Consumer<Exception> handler) {
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
                    handler.accept(null);
                }, handler));
            }, handler));
        }
    }

}
