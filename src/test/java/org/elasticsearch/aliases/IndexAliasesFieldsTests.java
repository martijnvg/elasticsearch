/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.aliases;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.cluster.metadata.AliasFieldsFiltering;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

/**
 */
public class IndexAliasesFieldsTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testGetApi() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string", "field2", "type=string")
                        .addAlias(new Alias("alias1").includeFields("field2"))
                        .addAlias(new Alias("alias2").includeFields("field1"))
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        GetResponse response = client().prepareGet("test", "type1", "1").get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(2));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));

        response = client().prepareGet("alias1", "type1", "1").get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(1));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));

        response = client().prepareGet("alias2", "type1", "1").get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(1));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));
    }

    @Test
    public void testMGetApi() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string", "field2", "type=string")
                        .addAlias(new Alias("alias1").includeFields("field2"))
                        .addAlias(new Alias("alias2").includeFields("field1"))
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        MultiGetResponse response = client().prepareMultiGet()
                .add("test", "type1", "1")
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));

        response = client().prepareMultiGet()
                .add("alias1", "type1", "1")
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));

        response = client().prepareMultiGet()
                .add("alias2", "type1", "1")
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));

        response = client().prepareMultiGet()
                .add("alias1", "type1", "1")
                .add("alias2", "type1", "1")
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));
        assertThat(response.getResponses()[1].isFailed(), is(false));
        assertThat(response.getResponses()[1].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[1].getResponse().getSource().size(), equalTo(1));
        assertThat(response.getResponses()[1].getResponse().getSource().get("field1").toString(), equalTo("value1"));
    }

    @Test
    public void testQuery() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string", "field2", "type=string")
                .addAlias(new Alias("alias1").includeFields("field2"))
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        SearchResponse response = client().prepareSearch("test").setQuery(matchQuery("field1", "value1")).get();
        assertHitCount(response, 1);
        response = client().prepareSearch("alias1").setQuery(matchQuery("field1", "value1")).get();
        assertHitCount(response, 0);

        client().admin().indices().prepareAliases().addAlias(new String[]{"test"}, "alias2", null, new AliasFieldsFiltering(new String[]{"field1"})).get();

        response = client().prepareSearch("test").setQuery(matchQuery("field2", "value2")).get();
        assertHitCount(response, 1);
        response = client().prepareSearch("alias2").setQuery(matchQuery("field2", "value2")).get();
        assertHitCount(response, 0);
    }

    @Test
    public void testCountApi() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string", "field2", "type=string")
                        .addAlias(new Alias("alias1").includeFields("field2"))
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        CountResponse response = client().prepareCount("test").setQuery(matchQuery("field1", "value1")).get();
        assertHitCount(response, 1);
        response = client().prepareCount("alias1").setQuery(matchQuery("field1", "value1")).get();
        assertHitCount(response, 0);

        client().admin().indices().prepareAliases().addAlias(new String[]{"test"}, "alias2", null, new AliasFieldsFiltering(new String[]{"field1"})).get();

        response = client().prepareCount("test").setQuery(matchQuery("field2", "value2")).get();
        assertHitCount(response, 1);
        response = client().prepareCount("alias2").setQuery(matchQuery("field2", "value2")).get();
        assertHitCount(response, 0);
    }

    @Test
    public void testFilters() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string", "field2", "type=string")
                        .addAlias(new Alias("alias1").includeFields("field2"))
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        SearchResponse response = client().prepareSearch("test").setPostFilter(termFilter("field1", "value1").cache(randomBoolean())).get();
        assertHitCount(response, 1);
        response = client().prepareSearch("alias1").setPostFilter(termFilter("field1", "value1").cache(randomBoolean())).get();
        assertHitCount(response, 0);

        client().admin().indices().prepareAliases().addAlias(new String[]{"test"}, "alias2", null, new AliasFieldsFiltering(new String[]{"field1"})).get();

        response = client().prepareSearch("test").setPostFilter(termFilter("field2", "value2").cache(randomBoolean())).get();
        assertHitCount(response, 1);
        response = client().prepareSearch("alias2").setPostFilter(termFilter("field2", "value2").cache(randomBoolean())).get();
        assertHitCount(response, 0);
    }

    @Test
    public void testFields() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string,store=yes", "field2", "type=string,store=yes")
                        .addAlias(new Alias("alias1").includeFields("field2"))
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        SearchResponse response = client().prepareSearch("test").addField("field1").get();
        assertThat(response.getHits().getAt(0).fields().get("field1").<String>getValue(), equalTo("value1"));
        response = client().prepareSearch("alias1").addField("field1").get();
        assertThat(response.getHits().getAt(0).fields().get("field1"), nullValue());

        client().admin().indices().prepareAliases().addAlias(new String[]{"test"}, "alias2", null, new AliasFieldsFiltering(new String[]{"field1"})).get();

        response = client().prepareSearch("test").addField("field2").get();
        assertThat(response.getHits().getAt(0).fields().get("field2").<String>getValue(), equalTo("value2"));
        response = client().prepareSearch("alias2").addField("field2").get();
        assertThat(response.getHits().getAt(0).fields().get("field2"), nullValue());
    }

    @Test
    public void testSource() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string", "field2", "type=string")
                        .addAlias(new Alias("alias1").includeFields("field2"))
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        SearchResponse response = client().prepareSearch("test").get();
        assertThat((String) response.getHits().getAt(0).sourceAsMap().get("field1"), equalTo("value1"));
        response = client().prepareSearch("alias1").get();
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field1"), nullValue());

        client().admin().indices().prepareAliases().addAlias(new String[]{"test"}, "alias2", null, new AliasFieldsFiltering(new String[]{"field1"})).get();

        response = client().prepareSearch("test").get();
        assertThat((String) response.getHits().getAt(0).sourceAsMap().get("field2"), equalTo("value2"));
        response = client().prepareSearch("alias2").get();
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field2"), nullValue());
    }

    @Test
    public void testSort() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=long", "field2", "type=long")
                        .addAlias(new Alias("alias1").includeFields("field2"))
        );

        client().prepareIndex("test", "type1", "1").setSource("field1", 1d, "field2", 2d)
                .setRefresh(true)
                .get();

        SearchResponse response = client().prepareSearch("test").addSort("field1", SortOrder.ASC).get();
        assertThat((Long) response.getHits().getAt(0).sortValues()[0], equalTo(1l));
        response = client().prepareSearch("alias1").addSort("field1", SortOrder.ASC).get();
        assertThat((Long) response.getHits().getAt(0).sortValues()[0], equalTo(Long.MAX_VALUE));

        client().admin().indices().prepareAliases().addAlias(new String[]{"test"}, "alias2", null, new AliasFieldsFiltering(new String[]{"field1"})).get();

        response = client().prepareSearch("test").addSort("field2", SortOrder.ASC).get();
        assertThat((Long) response.getHits().getAt(0).sortValues()[0], equalTo(2l));
        response = client().prepareSearch("alias2").addSort("field2", SortOrder.ASC).get();
        assertThat((Long) response.getHits().getAt(0).sortValues()[0], equalTo(Long.MAX_VALUE));
    }

    @Test
    public void testAggs() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string", "field2", "type=string")
                        .addAlias(new Alias("alias1").includeFields("field2"))
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        SearchResponse response = client().prepareSearch("test").addAggregation(AggregationBuilders.terms("_name").field("field1")).get();
        assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value1").getDocCount(), equalTo(1l));
        response = client().prepareSearch("alias1").addAggregation(AggregationBuilders.terms("_name").field("field1")).get();
        assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value1"), nullValue());

        client().admin().indices().prepareAliases().addAlias(new String[]{"test"}, "alias2", null, new AliasFieldsFiltering(new String[]{"field1"})).get();


        response = client().prepareSearch("test").addAggregation(AggregationBuilders.terms("_name").field("field2")).get();
        assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value2").getDocCount(), equalTo(1l));
        response = client().prepareSearch("alias2").addAggregation(AggregationBuilders.terms("_name").field("field2")).get();
        assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value2"), nullValue());
    }

    @Test
    public void testTVApi() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string,term_vector=with_positions_offsets_payloads", "field2", "type=string,term_vector=with_positions_offsets_payloads")
                        .addAlias(new Alias("alias1").includeFields("field2"))
                        .addAlias(new Alias("alias2").includeFields("field1"))
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        TermVectorsResponse response = client().prepareTermVectors("test", "type1", "1").get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(2));
        assertThat(response.getFields().terms("field1").size(), equalTo(1l));
        assertThat(response.getFields().terms("field2").size(), equalTo(1l));

        response = client().prepareTermVectors("alias1", "type1", "1").get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(1));
        assertThat(response.getFields().terms("field2").size(), equalTo(1l));

        response = client().prepareTermVectors("alias2", "type1", "1").get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(1));
        assertThat(response.getFields().terms("field1").size(), equalTo(1l));
    }

    @Test
    public void testMTVApi() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string,term_vector=with_positions_offsets_payloads", "field2", "type=string,term_vector=with_positions_offsets_payloads")
                        .addAlias(new Alias("alias1").includeFields("field2"))
                        .addAlias(new Alias("alias2").includeFields("field1"))
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        MultiTermVectorsResponse response = client().prepareMultiTermVectors().add("test", "type1", "1").get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1l));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1l));

        response = client().prepareMultiTermVectors().add("alias1", "type1", "1").get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1l));

        response = client().prepareMultiTermVectors().add("alias2", "type1", "1").get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1l));
    }

    @Test
    public void testRandom() throws Exception {
        int numFields = scaledRandomIntBetween(5, 50);
        String[] fields = new String[numFields];
        Set<String> includes = new HashSet<>();
        Set<String> excludes = new HashSet<>();
        Map<String, Object> doc = new HashMap<>();
        int j = 0;
        for (int i = 0; i < fields.length; i++) {
            fields[i] = "field" + i;
            if (i % 2 == 0) {
                includes.add(fields[i]);
            } else {
                excludes.add(fields[i]);
            }
            doc.put(fields[i], String.valueOf(j++));
        }

        String[] fieldMappers = new String[fields.length * 2];
        j = 0;
        for (String field : fields) {
            fieldMappers[j++] = field;
            fieldMappers[j++] = "type=string";
        }

        createIndex("test", client().admin().indices().prepareCreate("test")
                        .addMapping("type1", fieldMappers)
                        .addAlias(new Alias("alias1").includeFields(includes.toArray(new String[0])))
        );
        client().prepareIndex("test", "type1", "1").setSource(doc).setRefresh(true).get();

        // test alias1 --> include fields should yield a match
        for (String include : includes) {
            SearchResponse response = client().prepareSearch("alias1").setQuery(matchQuery(include, doc.get(include))).get();
            assertHitCount(response, 1);
        }
        for (String exclude : excludes) {
            SearchResponse response = client().prepareSearch("alias1").setQuery(matchQuery(exclude, doc.get(exclude))).get();
            assertHitCount(response, 0);
        }
    }

}
