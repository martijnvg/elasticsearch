/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;
import static org.hamcrest.Matchers.equalTo;

public class CcrRollingUpgradeIT extends AbstractMultiClusterUpgradeTestCase {

    public void testIndexFollowing() throws Exception {
        logger.info("clusterName={}, upgradeState={}", clusterName, upgradeState);

        if (clusterName == ClusterName.LEADER) {
            if (upgradeState == UpgradeState.NONE) {
                createLeaderIndex("leader_index1");
                index(leaderClient(), "leader_index1", 64);
                createLeaderIndex("leader_index2");
                index(leaderClient(), "leader_index2", 64);
            } else {
                throw new AssertionError("unexpected upgrade_state [" + upgradeState + "]");
            }
        } else if (clusterName == ClusterName.FOLLOWER) {
            switch (upgradeState) {
                case NONE:
                    followIndex(followerClient(), "leader", "leader_index1", "follower_index1");
                    assertBusy(() -> verifyTotalHitCount("follower_index1", 64, followerClient()));
                    break;
                case ONE_THIRD:
                    index(leaderClient(), "leader_index1", 64);
                    assertBusy(() -> verifyTotalHitCount("follower_index1", 128, followerClient()));
                    break;
                case TWO_THIRD:
                    index(leaderClient(), "leader_index1", 64);
                    assertBusy(() -> verifyTotalHitCount("follower_index1", 192, followerClient()));
                    break;
                case ALL:
                    index(leaderClient(), "leader_index1", 64);
                    assertBusy(() -> verifyTotalHitCount("follower_index1", 256, followerClient()));
                    // At this point the leader cluster has not been upgraded, but follower cluster has been upgrade.
                    // Create a leader index in the follow cluster and try to follow it in the leader cluster.
                    // This should fail, because the leader cluster at this point in time can't do file based recovery from follower.
//                    createLeaderIndex("leader_index3");
//                    index(followerClient, "leader_index3", 64);
                    break;
                default:
                    throw new AssertionError("unexpected upgrade_state [" + upgradeState + "]");
            }
        } else {
            throw new AssertionError("unexpected cluster_name [" + clusterName + "]");
        }
    }

    private static void createLeaderIndex(String indexName) throws IOException {
        Settings indexSettings = Settings.builder()
            .put("index.soft_deletes.enabled", true)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();
        createIndex(indexName, indexSettings);
    }

    private static void followIndex(RestClient client, String leaderCluster, String leaderIndex, String followIndex) throws IOException {
        final Request request = new Request("PUT", "/" + followIndex + "/_ccr/follow?wait_for_active_shards=1");
        request.setJsonEntity("{\"remote_cluster\": \"" + leaderCluster + "\", \"leader_index\": \"" + leaderIndex +
            "\", \"read_poll_timeout\": \"10ms\"}");
        assertOK(client.performRequest(request));
    }

    private static void index(RestClient client, String index, int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            final Request request = new Request("POST", "/" + index + "/_doc/");
            request.setJsonEntity("{}");
            assertOK(client.performRequest(request));
        }
    }

    private static void verifyTotalHitCount(final String index,
                                            final int expectedTotalHits,
                                            final RestClient client) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_search");
        request.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        Map<?, ?> response = toMap(client.performRequest(request));
        final int totalHits = (int) XContentMapValues.extractValue("hits.total", response);
        assertThat(totalHits, equalTo(expectedTotalHits));
    }

}
