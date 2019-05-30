package org.elasticsearch.xpack.enrich;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numClientNodes = 0, numDataNodes = 0)
public class DummyTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(EnrichPlugin.class);
    }

    public void test() {
        Settings masterNodeSettings = Settings.builder()
            .put("node.attr.my_name", "dedicated_master_node")
            .put(Node.NODE_MASTER_SETTING.getKey(), true)
            .put(Node.NODE_DATA_SETTING.getKey(), false)
            .put(Node.NODE_INGEST_SETTING.getKey(), false)
            .build();
        internalCluster().startNode(masterNodeSettings);
        Settings ingestNodeSettings = Settings.builder()
            .put("node.attr.my_name", "dedicated_ingest_node")
            .put(Node.NODE_MASTER_SETTING.getKey(), false)
            .put(Node.NODE_DATA_SETTING.getKey(), false)
            .put(Node.NODE_INGEST_SETTING.getKey(), true)
            .build();
        internalCluster().startNode(ingestNodeSettings);

        String indexName = EnrichPolicy.getBaseName("1");
        Settings indexSettings = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.routing.allocation.enable", "none")
            .build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        ClusterState clusterState = clusterService().state();
        logger.info(Strings.toString(clusterState, true, true));

        index(indexName, "_doc", "1", "{}");
        refresh(indexName);

        SearchResponse searchResponse = client().search(new SearchRequest(indexName)).actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getTotalHits().relation, equalTo(TotalHits.Relation.EQUAL_TO));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));

        GetResponse getResponse = client().get(new GetRequest(indexName, "1")).actionGet();
        assertThat(getResponse.isExists(), is(true));
    }

}
