/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ActionTransportException;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsRequest;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

final class FollowShardTracker {

    private static final Logger LOGGER = Loggers.getLogger(FollowShardTracker.class);
    private static final int PROCESSOR_RETRY_LIMIT = 5;
    private static final TimeValue RETRY_TIMEOUT = TimeValue.timeValueMillis(500);

    private final Client leaderClient;
    private final Client followerClient;
    private final BiFunction<TimeValue, Runnable, ScheduledFuture<?>> scheduler;
    private final BiConsumer<Long, Consumer<Exception>> versionChecker;

    private final ShardFollowTask params;
    private final ShardFollowNodeTask nodeTask;

    private volatile long leaderGlobalCheckpoint = -1;
    private volatile long lastReadSeqNo = -1;
    private volatile long lastWrittenSeqNo = -1;

    private final Map<Integer, Reader> readers = new HashMap<>();
    private final Map<Integer, Writer> writers = new HashMap<>();
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
        Reader reader = newReader();
        LOGGER.info("{}[{}] starting", params.getFollowShardId(), reader.id);
        reader.read(followGlobalCheckPoint);
        scheduler.apply(TimeValue.timeValueMillis(500), this::schedule);
    }

    private void schedule() {
        if (nodeTask.isRunning()) {
            if (readerCounter.get() == 0) {
                synchronized (this) {
                    Reader reader = newReader();
                    LOGGER.info("{}[{}] re-starting", params.getFollowShardId(), reader.id);
                    reader.read(lastReadSeqNo);
                }
            } else {
                LOGGER.debug("{} not restarting", params.getFollowShardId());
            }
            scheduler.apply(TimeValue.timeValueMillis(500), this::schedule);
        } else {
            LOGGER.debug("{} shard follow task has been stopped", params.getFollowShardId());
        }
    }

    private void updateWriteQueue(Reader reader, Translog.Operation[] ops) {
        assert Thread.holdsLock(this);
        if (ops.length != 0) {
            LOGGER.debug("{}[{}] received [{}] new ops", params.getFollowShardId(), reader.id, ops.length);
            writeQueue.addAll(Arrays.asList(ops));
            lastReadSeqNo = Math.max(lastReadSeqNo, ops[ops.length - 1].seqNo());
        } else {
            LOGGER.debug("{}[{}] received no new ops", params.getFollowShardId(), reader.id);
        }
        maybeStartWriting(false);
    }

    private void coordinate(Reader reader, long leaderGlobalCheckpoint) {
        assert Thread.holdsLock(this);
        this.leaderGlobalCheckpoint = Math.max(this.leaderGlobalCheckpoint, leaderGlobalCheckpoint);
        if (nodeTask.isRunning() == false) {
            LOGGER.info("{}[{}] stopping reader, because shard follow task has been stopped", params.getFollowShardId(), reader.id);
            deleteReader(reader);
            return;
        }

        long from = -1;
        for (Reader otherReaders : readers.values()) {
            from = Math.max(from, otherReaders.fromSeqNo);
        }

        if (from >= leaderGlobalCheckpoint) {
            LOGGER.info("{}[{}] stopping reader, nothing more to fetch [{}/{}]",
                params.getFollowShardId(), reader.id, from, leaderGlobalCheckpoint);
            deleteReader(reader);
            return;
        }

        from += 1;
        LOGGER.debug("{}[{}] existing reader [{}][{}]", params.getFollowShardId(), reader.id, lastReadSeqNo, leaderGlobalCheckpoint);
        reader.read(from);

        from += params.getMaxReadSize();
        while (from <= leaderGlobalCheckpoint &&
            readers.size() <= params.getMaxConcurrentReaders()) {
            Reader newReader = newReader();
            LOGGER.info("{}[{}] new reader [{}][{}]", params.getFollowShardId(), newReader.id, lastReadSeqNo, leaderGlobalCheckpoint);
            newReader.read(from);
            from += params.getMaxReadSize();
        }
    }

    private void maybeFlushWriteQueue(Writer writer) {
        assert Thread.holdsLock(this);
        if (shouldFlush()) {
            LOGGER.info("{}[{}] Flush threshold met", params.getFollowShardId(), writer.id);
            flushWriteQueue(writer);
        } else {
            deleteWriter(writer);
        }
    }

    private void maybeStartWriting(boolean force) {
        assert Thread.holdsLock(this);
        if (force && writers.isEmpty() && writeQueue.isEmpty() == false) {
            Writer writer = newWriter();
            LOGGER.info("{}[{}] Force flush, new writer", params.getFollowShardId(), writer.id);
            flushWriteQueue(writer);
        } else if (writers.isEmpty() && shouldFlush()) {
            Writer writer = newWriter();
            LOGGER.info("{}[{}] Flush threshold met, new writer", params.getFollowShardId(), writer.id);
            flushWriteQueue(writer);
        }
    }

    private boolean shouldFlush()  {
        assert Thread.holdsLock(this);
        boolean shouldFlush = writeQueue.size() > 1000;
        if (shouldFlush == false) {
            shouldFlush = writeQueue.stream().mapToLong(Translog.Operation::estimateSize).sum() > ByteSizeUnit.MB.toBytes(1);
        }
        return shouldFlush;
    }

    private void flushWriteQueue(Writer writer) {
        assert Thread.holdsLock(this);
        LOGGER.info("{}[{}]  Flushing [{}] write operations", params.getFollowShardId(), writer.id, writeQueue.size());
        Translog.Operation[] ops = writeQueue.toArray(new Translog.Operation[0]);
        writeQueue.clear();
        writer.write(ops, bulkShardOperationsResponse -> {
            long highestSeqNo = Arrays.stream(ops).mapToLong(Translog.Operation::seqNo).max().getAsLong();
            if (highestSeqNo > lastWrittenSeqNo) {
                lastWrittenSeqNo = highestSeqNo;
                nodeTask.updateProcessedGlobalCheckpoint(lastWrittenSeqNo);
            }
            synchronized (this) {
                maybeFlushWriteQueue(writer);
            }
        });
    }

    final class Reader {

        final int id;
        final AtomicInteger retryCounter = new AtomicInteger(0);

        long fromSeqNo;

        Reader(int id) {
            this.id = id;
        }

        void read(long fromSeqNo) {
            assert Thread.holdsLock(FollowShardTracker.this);
            this.fromSeqNo = fromSeqNo;
            ShardChangesAction.Request request = new ShardChangesAction.Request(params.getLeaderShardId());
            request.setFromSeqNo(Math.max(0, fromSeqNo));
            request.setSize(params.getMaxReadSize());
            LOGGER.debug("{}[{}] read [{}][{}]", params.getFollowShardId(), id, request.getFromSeqNo(), request.getSize());
            leaderClient.execute(ShardChangesAction.INSTANCE, request, new ActionListener<ShardChangesAction.Response>() {
                @Override
                public void onResponse(ShardChangesAction.Response response) {
                    handleShardChangesResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    handleFailure(retryCounter, e, () -> read(fromSeqNo));
                }
            });
        }

        void handleShardChangesResponse(ShardChangesAction.Response response) {
            versionChecker.accept(response.getIndexMetadataVersion(), e -> {
                if (e == null) {
                    // Reset retry counter, because we should only fail if errors happen continuously:
                    retryCounter.set(0);
                    synchronized (FollowShardTracker.this) {
                        FollowShardTracker.this.updateWriteQueue(this, response.getOperations());
                        FollowShardTracker.this.coordinate(this, response.getLeaderGlobalCheckpoint());
                    }
                } else {
                    handleFailure(retryCounter, e, () -> handleShardChangesResponse(response));
                }
            });
        }

    }

    private final AtomicInteger readerCounter = new AtomicInteger(0);

    private Reader newReader() {
        assert Thread.holdsLock(this);
        Reader reader = new Reader(readerCounter.getAndIncrement());
        Reader previous = readers.put(reader.id, reader);
        assert previous == null;
        return reader;
    }

    private void deleteReader(Reader reader) {
        assert Thread.holdsLock(this);
        Reader previous = readers.remove(reader.id);
        assert previous == reader;
        int result = readerCounter.decrementAndGet();
        assert result >= 0;
        if (result == 0 && writeQueue.isEmpty() == false) {
            maybeStartWriting(true);
        }
    }

    final class Writer {

        final int id;
        final AtomicInteger retryCounter = new AtomicInteger(0);

        Writer(int id) {
            this.id = id;
        }

        private void write(Translog.Operation[] operations, Consumer<BulkShardOperationsResponse> responseHandler) {
            assert Thread.holdsLock(FollowShardTracker.this);
            final BulkShardOperationsRequest request = new BulkShardOperationsRequest(params.getFollowShardId(), operations);
            followerClient.execute(BulkShardOperationsAction.INSTANCE, request,
                new ActionListener<BulkShardOperationsResponse>() {
                    @Override
                    public void onResponse(BulkShardOperationsResponse response) {
                        retryCounter.set(0);
                        responseHandler.accept(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        handleFailure(retryCounter, e, () -> write(operations, responseHandler));
                    }
                }
            );
        }
    }

    private final AtomicInteger writerCounter = new AtomicInteger(0);

    private Writer newWriter() {
        assert Thread.holdsLock(this);
        Writer writer = new Writer(writerCounter.getAndIncrement());
        Writer previous = writers.put(writer.id, writer);
        assert previous == null;
        return writer;
    }

    private void deleteWriter(Writer writer) {
        assert Thread.holdsLock(this);
        Writer previous = writers.remove(writer.id);
        assert previous == writer;
        int result = writerCounter.decrementAndGet();
        assert result >= 0;
    }

    private void handleFailure(AtomicInteger retryCounter, Exception e, Runnable task) {
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
