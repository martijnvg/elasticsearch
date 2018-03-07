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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.InetAddressRange;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.net.InetAddress;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public class IngestGeoLite2Processor extends AbstractProcessor {

    public static final String TYPE = "geolite2";

    private final String field;
    private final String targetField;
    private final boolean ignoreMissing;
    private final Set<Property> properties;
    private final IndexSearcher indexSearcher;

    private IngestGeoLite2Processor(String tag, String field, String targetField, boolean ignoreMissing, Set<Property> properties,
                                    IndexSearcher indexSearcher) {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.properties = properties;
        this.indexSearcher = indexSearcher;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        String ip = ingestDocument.getFieldValue(field, String.class, ignoreMissing);
        if (ip == null && ignoreMissing) {
            return;
        } else if (ip == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot extract geoip information.");
        }

        InetAddress ipAddress = InetAddresses.forString(ip);
        Query query = InetAddressRange.newIntersectsQuery("network", ipAddress, ipAddress);
        TopDocs topDocs = indexSearcher.search(query, 1);
        if (topDocs.totalHits != 0) {
            assert topDocs.totalHits == 1;
            Document document = indexSearcher.doc(topDocs.scoreDocs[0].doc);
            Map<String, Object> geoData = new HashMap<>();
            for (Property property : this.properties) {
                switch (property) {
                    case IP:
                        geoData.put("ip", NetworkAddress.format(ipAddress));
                        break;
                    case COUNTRY_ISO_CODE:
                        String countryIsoCode = document.get(Property.COUNTRY_ISO_CODE.getFieldName());
                        if (countryIsoCode != null) {
                            geoData.put("country_iso_code", countryIsoCode);
                        }
                        break;
                    case COUNTRY_NAME:
                        String countryName = document.get(Property.COUNTRY_NAME.getFieldName());
                        if (countryName != null) {
                            geoData.put("country_name", countryName);
                        }
                        break;
                    case CONTINENT_NAME:
                        String continentName = document.get(Property.CONTINENT_NAME.getFieldName());
                        if (continentName != null) {
                            geoData.put("continent_name", continentName);
                        }
                        break;
                    case REGION_NAME:
                        String subdivisionName = document.get(Property.REGION_NAME.getFieldName());
                        if (subdivisionName != null) {
                            geoData.put("region_name", subdivisionName);
                        }
                        break;
                    case CITY_NAME:
                        String cityName = document.get(Property.CITY_NAME.getFieldName());
                        if (cityName != null) {
                            geoData.put("city_name", cityName);
                        }
                        break;
                    case TIMEZONE:
                        String locationTimeZone = document.get(Property.TIMEZONE.getFieldName());
                        if (locationTimeZone != null) {
                            geoData.put("timezone", locationTimeZone);
                        }
                        break;
                    case LOCATION:
                        String latitude = document.get("latitude");
                        String longitude = document.get("longitude");
                        if (latitude != null && longitude != null) {
                            Map<String, Object> locationObject = new HashMap<>();
                            locationObject.put("lat", Double.parseDouble(latitude));
                            locationObject.put("lon", Double.parseDouble(longitude));
                            geoData.put("location", locationObject);
                        }
                        break;
                }
            }
            ingestDocument.setFieldValue(targetField, geoData);
        }

    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory {

        private final IndexSearcher indexSearcher;

        Factory(IndexSearcher indexSearcher) {
            this.indexSearcher = indexSearcher;
        }

        @Override
        public Processor create(Map<String, Processor.Factory> processorFactories, String tag, Map<String, Object> config) {
            String ipField = readStringProperty(TYPE, tag, config, "field");
            String targetField = readStringProperty(TYPE, tag, config, "target_field", "geoip");
            boolean ignoreMissing = readBooleanProperty(TYPE, tag, config, "ignore_missing", false);
            List<String> propertyNames = readOptionalList(TYPE, tag, config, "properties");

            final Set<Property> properties;
            if (propertyNames != null) {
                properties = EnumSet.noneOf(Property.class);
                for (String propertyValue : propertyNames) {
                    try {
                        Property property = Property.valueOf(propertyValue.toUpperCase(Locale.ROOT));
                        properties.add(property);
                    } catch (IllegalArgumentException e) {
                        throw newConfigurationException(TYPE, tag, "properties", e.getMessage());
                    }
                }
            } else {
                properties = EnumSet.allOf(Property.class);
            }
            return new IngestGeoLite2Processor(tag, ipField, targetField, ignoreMissing, properties, indexSearcher);
        }
    }

    public enum Property {

        IP("ip"),
        COUNTRY_ISO_CODE("country_iso_code"),
        COUNTRY_NAME("country_name"),
        CONTINENT_NAME("continent_name"),
        REGION_NAME("region_name"),
        CITY_NAME("city_name"),
        TIMEZONE("time_zone"),
        LOCATION("location");

        private final String fieldName;

        Property(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldName() {
            return fieldName;
        }

    }

}
