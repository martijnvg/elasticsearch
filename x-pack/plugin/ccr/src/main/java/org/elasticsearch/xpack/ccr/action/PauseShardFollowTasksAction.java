package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.transport.TransportResponse.Empty;
import static org.elasticsearch.xpack.ccr.action.TransportFollowStatsAction.findFollowerIndicesFromShardFollowTasks;

public class PauseShardFollowTasksAction extends Action<PauseShardFollowTasksAction.Response> {

    public static final PauseShardFollowTasksAction INSTANCE = new PauseShardFollowTasksAction();
    public static final String NAME = "cluster:admin/ccr/pause_follow_shards";

    private PauseShardFollowTasksAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    public static class Request extends BaseTasksRequest<Request> {

        private final String followerIndex;

        public Request(String followerIndex) {
            this.followerIndex = followerIndex;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.followerIndex = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(followerIndex);
        }
    }

    public static class Response extends BaseTasksResponse {

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(List<TaskOperationFailure> taskFailures, List<? extends ElasticsearchException> nodeFailures) {
            super(taskFailures, nodeFailures);
        }
    }

    public static class TaskResponse implements Writeable {

        private final boolean success;

        public TaskResponse(boolean success) {
            this.success = success;
        }

        public TaskResponse(StreamInput in) throws IOException {
            this.success = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(success);
        }
    }

    public static class TransportAction extends TransportTasksAction<ShardFollowNodeTask, Request, Response, TaskResponse> {

        @Inject
        public TransportAction(ClusterService clusterService, TransportService transportService, ActionFilters actionFilters) {
            super(NAME, clusterService, transportService, actionFilters, Request::new, Response::new, TaskResponse::new,
                Ccr.CCR_THREAD_POOL_NAME);
        }

        @Override
        protected Response newResponse(Request request, List<TaskResponse> tasks, List<TaskOperationFailure> taskOperationFailures,
                                       List<FailedNodeException> failedNodeExceptions) {
            return new Response(taskOperationFailures, failedNodeExceptions);
        }

        @Override
        protected void processTasks(final Request request, final Consumer<ShardFollowNodeTask> operation) {
            final ClusterState state = clusterService.state();
            final Set<String> followerIndices = findFollowerIndicesFromShardFollowTasks(state, request.followerIndex);

            for (final Task task : taskManager.getTasks().values()) {
                if (task instanceof ShardFollowNodeTask) {
                    final ShardFollowNodeTask shardFollowNodeTask = (ShardFollowNodeTask) task;
                    if (followerIndices.contains(shardFollowNodeTask.getFollowShardId().getIndexName()) &&
                            shardFollowNodeTask.isStopped()) {
                        operation.accept(shardFollowNodeTask);
                    }
                }
            }
        }

        @Override
        protected void taskOperation(Request request, ShardFollowNodeTask task, ActionListener<TaskResponse> listener) {
            task.stop();
            listener.onResponse(new TaskResponse(true));
        }
    }

}
