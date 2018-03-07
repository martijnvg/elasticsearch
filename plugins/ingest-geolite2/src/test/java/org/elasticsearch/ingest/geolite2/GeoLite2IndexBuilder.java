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
package org.elasticsearch.ingest.geolite2;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.codecs.lucene70.Lucene70Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.InetAddressRange;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.ingest.geolite2.IngestGeoLite2Processor.Property;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.BufferedReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressForbidden(reason = "using PathUtils.get(...) and system.out.println(...) is ok for this command line utility")
public class GeoLite2IndexBuilder {

    public static void main(String[] args) throws Exception {
        Path locationsFile = PathUtils.get(args[0]);
        Path ip4BlocksFile = PathUtils.get(args[1]);
        Path ip6BlocksFile = PathUtils.get(args[2]);
        Path indexDirectory = PathUtils.get(args[3]);

        CsvPreference csvPref = CsvPreference.STANDARD_PREFERENCE;
        Map<String, Map<String, String>> locations = new HashMap<>();

        try (BufferedReader reader = Files.newBufferedReader(locationsFile, StandardCharsets.UTF_8)) {
            try (CsvListReader csvReader = new CsvListReader(reader, csvPref)) {
                String[] header = csvReader.getHeader(true);
                for (List<String> line = csvReader.read(); line != null; line = csvReader.read()) {
                    assert header.length == line.size();
                    String geoNameId = line.get(0);
                    Map<String, String> entry = new HashMap<>();
                    for (int i = 1; i < line.size(); i++) {
                        String key = header[i];
                        entry.put(key, line.get(i));
                    }
                    locations.put(geoNameId, entry);
                    Document document = new Document();
                    for (Map.Entry<String, String> entries : entry.entrySet()) {
                        if (entries.getValue() != null) {
                            document.add(new StoredField(entries.getKey(), entries.getValue()));
                        }
                    }
                }
            }
        }

        try (Directory directory = FSDirectory.open(indexDirectory)) {
            IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            config.getMergePolicy().setNoCFSRatio(1d);
            config.setCodec(new Lucene70Codec(Lucene50StoredFieldsFormat.Mode.BEST_COMPRESSION));
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (Path blocksFile : new Path[]{ip4BlocksFile, ip6BlocksFile}) {
                    try (BufferedReader reader = Files.newBufferedReader(blocksFile, StandardCharsets.UTF_8)) {
                        try (CsvListReader csvReader = new CsvListReader(reader, csvPref)) {
                            String[] header = csvReader.getHeader(true);
                            for (List<String> line = csvReader.read(); line != null; line = csvReader.read()) {
                                assert header.length == line.size();
                                String network = line.get(0);
                                InetAddress[] minMax = parseMinMaxFromCidr(network);
                                Document document = new Document();
                                document.add(new InetAddressRange("network", minMax[0], minMax[1]));

                                String geoNameId = line.get(1);
                                Map<String, String> location = locations.get(geoNameId);
                                if (location == null) {
                                    geoNameId = line.get(2);
                                    location = locations.get(geoNameId);
                                }
                                if (location == null) {
                                    System.out.println("No geoNameId for line [" + line + "]");
                                    continue;
                                }

                                if (line.get(7) != null) {
                                    document.add(new StoredField("latitude", line.get(7)));
                                }
                                if (line.get(8) != null) {
                                    document.add(new StoredField("longitude", line.get(8)));
                                }

                                String value = location.get("country_iso_code");
                                if (value != null) {
                                    document.add(new StoredField(Property.COUNTRY_ISO_CODE.getFieldName(), value));
                                }

                                value = location.get("country_name");
                                if (value != null) {
                                    document.add(new StoredField(Property.COUNTRY_NAME.getFieldName(), value));
                                }

                                value = location.get("continent_name");
                                if (value != null) {
                                    document.add(new StoredField(Property.CONTINENT_NAME.getFieldName(), value));
                                }

                                value = location.get("subdivision_1_name");
                                if (value != null) {
                                    if (location.get("subdivision_2_name") != null) {
                                        value += " / " + location.get("subdivision_2_name");
                                    }
                                    document.add(new StoredField(Property.REGION_NAME.getFieldName(), value));
                                }

                                value = location.get("city_name");
                                if (value != null) {
                                    document.add(new StoredField(Property.CITY_NAME.getFieldName(), value));
                                }

                                value = location.get("time_zone");
                                if (value != null) {
                                    document.add(new StoredField(Property.TIMEZONE.getFieldName(), value));
                                }

                                writer.addDocument(document);
                            }
                        }
                    }
                }
                writer.forceMerge(1);
            }
        }

    }

    private static InetAddress[] parseMinMaxFromCidr(String network) throws Exception {
        final Tuple<InetAddress, Integer> cidr = InetAddresses.parseCidr(network);
        // create the lower value by zeroing out the host portion, upper value by filling it with all ones.
        byte[] min = cidr.v1().getAddress();
        byte[] max = min.clone();
        for (int i = cidr.v2(); i < 8 * min.length; i++) {
            int m = 1 << 7 - (i & 7);
            min[i >> 3] &= ~m;
            max[i >> 3] |= m;
        }
        return new InetAddress[] {InetAddress.getByAddress(min), InetAddress.getByAddress(max)};
    }

}
