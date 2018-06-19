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
        Client leaderClient = wrapClient(params.getLeaderClusterAlias() != null ?
                this.client.getRemoteClusterClient(params.getLeaderClusterAlias()) : this.client, params);
        Client followerClient = wrapClient(this.client, params);
        IndexMetadataVersionChecker imdVersionChecker = new IndexMetadataVersionChecker(params.getLeaderShardId().getIndex(),
                params.getFollowShardId().getIndex(), client, leaderClient);
        logger.info("[{}] initial leader mapping with follower mapping syncing", params);
        BiFunction<TimeValue, Runnable, ScheduledFuture<?>> scheduler =
            (delay, command) -> threadPool.schedule(delay, ThreadPool.Names.GENERIC, command);
        ShardFollowTracker tracker =
            new ShardFollowTracker(leaderClient, followerClient, scheduler, imdVersionChecker, params, shardFollowNodeTask);
        imdVersionChecker.updateMapping(1L /* Force update, version is initially 0L */, e -> {
            if (e == null) {
                logger.info("Starting shard following [{}]", params);
                fetchGlobalCheckpoint(followerClient, params.getFollowShardId(),
                        followGlobalCheckPoint -> {
                            shardFollowNodeTask.updateProcessedGlobalCheckpoint(followGlobalCheckPoint);
                            tracker.start(followGlobalCheckPoint);
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

    static class ShardFollowTracker {

        private static final Logger LOGGER = Loggers.getLogger(ShardFollowTracker.class);

        private final Client leaderClient;
        private final Client followerClient;
        private final BiFunction<TimeValue, Runnable, ScheduledFuture<?>> scheduler;
        private final BiConsumer<Long, Consumer<Exception>> versionChecker;

        private final ShardFollowTask params;
        private final ShardFollowNodeTask nodeTask;

        private volatile long lastSeenSeqNo = -1;
        private volatile long lastProcessedSeqNo = -1;
        private final Queue<Translog.Operation> writeQueue = new LinkedList<>();
        private final Set<Object> ongoingReads = new HashSet<>();

        ShardFollowTracker(Client leaderClient, Client followerClient, BiFunction<TimeValue, Runnable, ScheduledFuture<?>> scheduler,
                           BiConsumer<Long, Consumer<Exception>> versionChecker, ShardFollowTask params, ShardFollowNodeTask nodeTask) {
            this.leaderClient = leaderClient;
            this.followerClient = followerClient;
            this.scheduler = scheduler;
            this.versionChecker = versionChecker;
            this.params = params;
            this.nodeTask = nodeTask;
        }

        synchronized void start(long followGlobalCheckPoint) {
            lastSeenSeqNo = followGlobalCheckPoint;
            Object token = new Object();
            ongoingReads.add(token);
            coordinate(token, lastSeenSeqNo + 1);
            scheduler.apply(TimeValue.timeValueMillis(3000), this::schedule);
        }

        synchronized void schedule() {
            if (ongoingReads.isEmpty() && nodeTask.isRunning()) {
                LOGGER.info("Re-starting");
                start(lastSeenSeqNo);
            }
        }

        synchronized void coordinate(Object token, long leaderGlobalCheckpoint) {
            assert token != null;
            if (nodeTask.isRunning() == false || lastSeenSeqNo >= leaderGlobalCheckpoint) {
                if (ongoingReads.size() == 1) {
                    flushWriteQueue(token);
                }
                ongoingReads.remove(token);
                return;
            }

            LOGGER.info("{}[{}] coordinate [{}][{}]", params.getFollowShardId(), token, lastProcessedSeqNo, leaderGlobalCheckpoint);
            long from = lastSeenSeqNo + 1;
            long size = params.getMaxReadSize();
            getWriteOperations(token, from, size, response -> handleShardChangesResponse(token, response));

            from += params.getMaxReadSize();
            while (from <= leaderGlobalCheckpoint && ongoingReads.size() <= params.getNumConcurrentReads()) {
                Object newToken = new Object();
                ongoingReads.add(newToken);
                getWriteOperations(token, from, size, response -> handleShardChangesResponse(newToken, response));
                from += params.getMaxReadSize();
            }
        }

        synchronized void handleShardChangesResponse(Object token, ShardChangesAction.Response response) {
            versionChecker.accept(response.getIndexMetadataVersion(), e -> {
                synchronized (this) {
                    if (e != null) {
                        handleFailure(token, e, () -> handleShardChangesResponse(token, response));
                    } else {
                        if (response.getOperations().length != 0) {
                            LOGGER.info("{}[{}] Received [{}/{}]", params.getFollowShardId(), token, response.getOperations()[0].seqNo(),
                                response.getHighestSeqNo());
                            writeQueue.addAll(Arrays.asList(response.getOperations()));
                            lastSeenSeqNo = Math.max(lastSeenSeqNo, response.getHighestSeqNo());
                            maybeFlushWriteQueue(token);
                            coordinate(token, response.getLeaderGlobalCheckpoint());
                        } else {
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
                flushWriteQueue(token);
            }
        }

        private void flushWriteQueue(Object token) {
            LOGGER.info("{}[{}]  Flushing [{}] write operations", params.getFollowShardId(), token, writeQueue.size());
            Translog.Operation[] ops = writeQueue.toArray(new Translog.Operation[0]);
            writeQueue.clear();
            persistWriteOperations(token, ops, bulkShardOperationsResponse -> {
                nodeTask.updateProcessedGlobalCheckpoint(lastSeenSeqNo);
                maybeFlushWriteQueue(token);
            });
        }

        private void getWriteOperations(Object token, long fromSeqNo, long size, Consumer<ShardChangesAction.Response> responseHandler) {
            ShardChangesAction.Request request = new ShardChangesAction.Request(params.getLeaderShardId());
            request.setFromSeqNo(fromSeqNo);
            request.setSize(size);
            LOGGER.info("{}[{}] getWriteOperations [{}][{}]", params.getFollowShardId(), token, fromSeqNo, size);
            leaderClient.execute(ShardChangesAction.INSTANCE, request, new ActionListener<ShardChangesAction.Response>() {
                @Override
                public void onResponse(ShardChangesAction.Response response) {
                   responseHandler.accept(response);
                }

                @Override
                public void onFailure(Exception e) {
                    handleFailure(token, e, () -> getWriteOperations(token, fromSeqNo, size, responseHandler));
                }
            });
        }

        private void persistWriteOperations(Object token, Translog.Operation[] operations,
                                            Consumer<BulkShardOperationsResponse> responseHandler) {
            final BulkShardOperationsRequest request = new BulkShardOperationsRequest(params.getFollowShardId(), operations);
            followerClient.execute(BulkShardOperationsAction.INSTANCE, request,
                new ActionListener<BulkShardOperationsResponse>() {
                    @Override
                    public void onResponse(BulkShardOperationsResponse response) {
                        responseHandler.accept(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        handleFailure(token, e, () -> persistWriteOperations(token, operations, responseHandler));
                    }
                }
            );
        }

        private final AtomicInteger retryCounter = new AtomicInteger(0);

        private void handleFailure(Object token, Exception e, Runnable task) {
            assert e != null;
            if (shouldRetry(e)) {
                if (nodeTask.isRunning() && retryCounter.incrementAndGet() <= PROCESSOR_RETRY_LIMIT) {
                    scheduler.apply(RETRY_TIMEOUT, task);
                } else {
                    ongoingReads.remove(token);
                    nodeTask.markAsFailed(new ElasticsearchException("retrying failed [" + retryCounter.get() +
                        "] times, aborting...", e));
                }
            } else {
                ongoingReads.remove(token);
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
                LOGGER.debug("Current index metadata version [{}] is higher or equal than minimum required index metadata version [{}]",
                    currentIndexMetadataVersion.get(), minimumRequiredIndexMetadataVersion);
                handler.accept(null);
            } else {
                updateMapping(minimumRequiredIndexMetadataVersion, handler);
            }
        }

        void updateMapping(long minimumRequiredIndexMetadataVersion, Consumer<Exception> handler) {
            if (currentIndexMetadataVersion.get() >= minimumRequiredIndexMetadataVersion) {
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
                    handler.accept(null);
                }, handler));
            }, handler));
        }
    }

}
