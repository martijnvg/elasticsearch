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
package org.elasticsearch.client;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.BuildInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.LicenseInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.elasticsearch.protocol.xpack.license.LicenseStatus;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.io.IOException;
import java.util.stream.Collectors;

public class XPackInfoResponseTests extends
        AbstractHlrcStreamableXContentTestCase<XPackInfoResponse, org.elasticsearch.client.xpack.XPackInfoResponse> {

    @Override
    protected XPackInfoResponse createBlankInstance() {
        return new XPackInfoResponse();
    }

    @Override
    public org.elasticsearch.client.xpack.XPackInfoResponse doHlrcParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.xpack.XPackInfoResponse.fromXContent(parser);
    }

    @Override
    public XPackInfoResponse convertHlrcToInternal(org.elasticsearch.client.xpack.XPackInfoResponse instance) {
        return new XPackInfoResponse(convertHlrcToInternal(instance.getBuildInfo()),
            convertHlrcToInternal(instance.getLicenseInfo()), convertHlrcToInternal(instance.getFeatureSetsInfo()));
    }

    private BuildInfo convertHlrcToInternal(org.elasticsearch.client.xpack.XPackInfoResponse.BuildInfo buildInfo) {
        return buildInfo != null ? new BuildInfo(buildInfo.getHash(), buildInfo.getTimestamp()) : null;
    }

    private LicenseInfo convertHlrcToInternal(org.elasticsearch.client.xpack.XPackInfoResponse.LicenseInfo licenseInfo) {
        return licenseInfo != null
            ? new LicenseInfo(licenseInfo.getUid(), licenseInfo.getType(), licenseInfo.getMode(),
                licenseInfo.getStatus() != null ? LicenseStatus.valueOf(licenseInfo.getStatus().name()) : null,
                licenseInfo.getExpiryDate())
            : null;
    }

    private FeatureSetsInfo convertHlrcToInternal(org.elasticsearch.client.xpack.XPackInfoResponse.FeatureSetsInfo featureSetsInfo) {
        return featureSetsInfo != null
            ? new FeatureSetsInfo(featureSetsInfo.getFeatureSets().values().stream()
            .map(fs -> new FeatureSet(fs.name(), fs.description(), fs.available(), fs.enabled(),
                fs.nativeCodeInfo()))
            .collect(Collectors.toSet()))
            : null;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return path -> path.equals("features")
                || (path.startsWith("features") && path.endsWith("native_code_info"));
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        Map<String, String> params = new HashMap<>();
        if (ESTestCase.randomBoolean()) {
            params.put("human", ESTestCase.randomBoolean() ? "true" : "false");
        }
        if (ESTestCase.randomBoolean()) {
            params.put("categories", "_none");
        }
        return new ToXContent.MapParams(params);
    }

    @Override
    protected XPackInfoResponse createTestInstance() {
        return new XPackInfoResponse(
            ESTestCase.randomBoolean() ? null : randomBuildInfo(),
            ESTestCase.randomBoolean() ? null : randomLicenseInfo(),
            ESTestCase.randomBoolean() ? null : randomFeatureSetsInfo());
    }

    @Override
    protected XPackInfoResponse mutateInstance(XPackInfoResponse response) {
        @SuppressWarnings("unchecked")
        Function<XPackInfoResponse, XPackInfoResponse> mutator = ESTestCase.randomFrom(
            r -> new XPackInfoResponse(
                    mutateBuildInfo(r.getBuildInfo()),
                    r.getLicenseInfo(),
                    r.getFeatureSetsInfo()),
            r -> new XPackInfoResponse(
                    r.getBuildInfo(),
                    mutateLicenseInfo(r.getLicenseInfo()),
                    r.getFeatureSetsInfo()),
            r -> new XPackInfoResponse(
                    r.getBuildInfo(),
                    r.getLicenseInfo(),
                    mutateFeatureSetsInfo(r.getFeatureSetsInfo())));
        return mutator.apply(response);
    }

    private BuildInfo randomBuildInfo() {
        return new BuildInfo(
            ESTestCase.randomAlphaOfLength(10),
            ESTestCase.randomAlphaOfLength(15));
    }

    private BuildInfo mutateBuildInfo(BuildInfo buildInfo) {
        if (buildInfo == null) {
            return randomBuildInfo();
        }
        return null;
    }

    private LicenseInfo randomLicenseInfo() {
        return new LicenseInfo(
            ESTestCase.randomAlphaOfLength(10),
            ESTestCase.randomAlphaOfLength(4),
            ESTestCase.randomAlphaOfLength(5),
            ESTestCase.randomFrom(LicenseStatus.values()),
            ESTestCase.randomLong());
    }

    private LicenseInfo mutateLicenseInfo(LicenseInfo licenseInfo) {
        if (licenseInfo == null) {
            return randomLicenseInfo();
        }
        return null;
    }

    private FeatureSetsInfo randomFeatureSetsInfo() {
        int size = ESTestCase.between(0, 10);
        Set<FeatureSet> featureSets = new HashSet<>(size);
        while (featureSets.size() < size) {
            featureSets.add(randomFeatureSet());
        }
        return new FeatureSetsInfo(featureSets);
    }

    private FeatureSetsInfo mutateFeatureSetsInfo(FeatureSetsInfo featureSetsInfo) {
        if (featureSetsInfo == null) {
            return randomFeatureSetsInfo();
        }
        return null;
    }

    private FeatureSet randomFeatureSet() {
        return new FeatureSet(
            ESTestCase.randomAlphaOfLength(5),
            ESTestCase.randomBoolean() ? null : ESTestCase.randomAlphaOfLength(20),
            ESTestCase.randomBoolean(),
            ESTestCase.randomBoolean(),
            randomNativeCodeInfo());
    }

    private Map<String, Object> randomNativeCodeInfo() {
        if (ESTestCase.randomBoolean()) {
            return null;
        }
        int size = ESTestCase.between(0, 10);
        Map<String, Object> nativeCodeInfo = new HashMap<>(size);
        while (nativeCodeInfo.size() < size) {
            nativeCodeInfo.put(ESTestCase.randomAlphaOfLength(5), ESTestCase.randomAlphaOfLength(5));
        }
        return nativeCodeInfo;
    }
}
