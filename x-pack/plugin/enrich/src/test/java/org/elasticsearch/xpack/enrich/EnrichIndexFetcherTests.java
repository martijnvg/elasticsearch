package org.elasticsearch.xpack.enrich;

import org.apache.lucene.document.Document;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.enrich.EnrichIndexProvider.LocalEnrichIndex;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class EnrichIndexFetcherTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(EnrichPlugin.class);
    }

    public void testFetch() throws Exception {
        ExecutorService genericTP = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(genericTP).execute(any(Runnable.class));

        EnrichIndexProvider provider = new EnrichIndexProvider(getInstanceFromNode(Environment.class));
        provider.doStart();
        EnrichIndexFetcher fetcher = new EnrichIndexFetcher(nodeSettings(), client(), genericTP, provider);

        client().index(new IndexRequest("my-index").id("1").source("{}", XContentType.JSON)).actionGet();
        client().admin().indices().refresh(new RefreshRequest("my-index")).actionGet();
        client().admin().indices().forceMerge(new ForceMergeRequest("my-index")).actionGet();
        ClusterState clusterState = client().admin().cluster().state(new ClusterStateRequest()).actionGet().getState();
        IndexMetaData imd = clusterState.metaData().index("my-index");

        Exception[] holder = new Exception[1];
        fetcher.fetch("my-policy", imd, e -> holder[0] = e);
        assertThat(holder[0], nullValue());
        LocalEnrichIndex result = provider.getLocalEnrichIndex("my-policy");
        assertThat(result, notNullValue());
        assertThat(result.getCurrentReader(), notNullValue());
        Document document = result.getCurrentReader().document(0);
        assertThat(document, notNullValue());
        assertThat(document.getBinaryValue("_id").utf8ToString(), equalTo("1"));
    }

}
