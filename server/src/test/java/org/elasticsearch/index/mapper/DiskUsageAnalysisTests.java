/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.lucene90.Lucene90CompoundFormat;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.*;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@TimeoutSuite(millis = 12 * 60 * 60 * 1000)
public class DiskUsageAnalysisTests extends ESTestCase {

    public void testOffsetAnalysis() throws Exception {
        final ByteArrayStreamInput scratch = new ByteArrayStreamInput();
        String fullPath = "/Users/mvg/temp/offset_data_dire/data/indices/-58uS6EbSbewM7FgL6lw2Q/0/index";
        try (var directory = FSDirectory.open(Path.of(fullPath))) {
            try (var reader = DirectoryReader.open(directory)) {
                long sum = 0L;
                int count = 0;
                for (var leafReaderContext : reader.leaves()) {
                    var leafReader = leafReaderContext.reader();
                    var sortedSetDocValues = leafReader.getSortedSetDocValues("tags");
                    if (sortedSetDocValues == null) {
                        continue;
                    }
                    var sortedDocValues = leafReader.getSortedDocValues("tags.offsets");
                    if (sortedDocValues == null) {
                        continue;
                    }
                    for (int docId = 0; docId < leafReader.maxDoc(); docId++) {
                        if (sortedSetDocValues.advanceExact(docId)) {
                            StringBuilder builder = new StringBuilder();
                            for (int i = 0; i < sortedSetDocValues.docValueCount(); i++) {
                                long ord = sortedSetDocValues.nextOrd();
                                builder.append(sortedSetDocValues.lookupOrd(ord).utf8ToString()).append(',');
                            }
                            // System.out.println("values: " + builder.toString());
                            if (sortedDocValues.advanceExact(docId)) {
                                var encodedValue = sortedDocValues.lookupOrd(sortedDocValues.ordValue());
                                sum += encodedValue.length;
                                count++;
                                scratch.reset(encodedValue.bytes, encodedValue.offset, encodedValue.length);
                                int[] offsetToOrd = FieldArrayContext.parseOffsetArray(scratch);
                                GrowableWriter writer;
                                var m = PackedInts.getMutable(offsetToOrd.length, 5, PackedInts.Format.PACKED);
                                for (int i = 0; i < offsetToOrd.length; i++) {
                                    m.set(i, offsetToOrd[i]);
                                }
                                // System.out.println("offsets: " + Arrays.toString(offsetToOrd));
                            } else {
                                System.out.println("no binary docvalues");
                            }
                        }
                    }
                }
                System.out.println("count: " + count);
                System.out.println("sum: " + sum);
                System.out.println("avg: " + (sum / count));
            }
        }
    }

    public void testIgnoredSourceAnalysis() throws Exception {
        Set<String> fieldNames = new HashSet<>();
        var indices = Files.list(Path.of("/Users/mvg/temp/offset_data_dire/data/indices")).toList();
        for (Path indexDir : indices) {
            Path fullPath = indexDir.resolve("0/index");
            System.out.println("index dir: " + fullPath.toAbsolutePath());
            System.out.println("fieldNames: " + fieldNames);
            try (var directory = FSDirectory.open(fullPath)) {
                try (var reader = DirectoryReader.open(directory)) {
                    for (var leafReaderContext : reader.leaves()) {
                        System.out.println("new segment");
                        var leafReader = leafReaderContext.reader();
                        var fieldInfo = leafReader.getFieldInfos().fieldInfo("_ignored_source");
                        if (fieldInfo == null) {
                            continue;
                        }

                        for (int docId = 0; docId < leafReader.maxDoc(); docId++) {
                            leafReader.storedFields().document(docId, new StoredFieldVisitor() {
                                @Override
                                public Status needsField(FieldInfo fieldInfo) throws IOException {
                                    return fieldInfo.getName().equals("_ignored_source") ? Status.YES : Status.NO;
                                }

                                @Override
                                public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
                                    var namedValue = IgnoredSourceFieldMapper.decodeAsMap(value).nameValue();
                                    fieldNames.add(namedValue.getFieldName());
                                }
                            });
                        }
                    }
                }
            }
        }
        System.out.println("fieldNames: " + fieldNames);
    }

    public void testES87TSDBDocValuesFormatAnalysis() throws Exception {
        String fullPath = "/Users/mvg/temp/otel-experiments/data-dir/indices/0Fx-NNq2ROe1QLFoZ4UF4A/0/index";
        try (var directory = FSDirectory.open(Path.of(fullPath))) {
            try (var reader = DirectoryReader.open(directory)) {
                for (var leafReaderContext : reader.leaves()) {
                    var leafReader = leafReaderContext.reader();

                    for (var field : leafReader.getFieldInfos()) {
                        if (field.getDocValuesType() == DocValuesType.NUMERIC || field.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
                            var dv = DocValues.unwrapSingleton(leafReader.getSortedNumericDocValues(field.getName()));
                            if (dv == null) {
                                System.out.println("multi valued field: " + field.getName());
                            }
                        }
                    }

                    for (FieldInfo fieldInfo : leafReader.getFieldInfos()) {
                        if (fieldInfo.getName().startsWith("_")) {
                            continue;
                        }
                        var sortedNumericDocValues = DocValues.unwrapSingleton(leafReader.getSortedNumericDocValues(fieldInfo.getName()));
                        if (sortedNumericDocValues == null) {
                            continue;
                        }

                        List<Integer> counts = new ArrayList<>();
                        int counter = 0;
                        long prev = -1;
                        long prevDiff = -1;
                        for (int docId = 0; docId < leafReader.maxDoc(); docId++) {
                            if (sortedNumericDocValues.advanceExact(docId)) {
                                long value = sortedNumericDocValues.longValue();
                                if (prev == -1) {
                                    prev = value;
                                    continue;
                                }

                                long diff = value - prev;
                                if (diff == prevDiff) {
                                    counter++;
                                } else {
                                    if (counter > 1) {
                                        counts.add(counter);
                                    }
                                    counter = 0;
                                }
                                prev = value;
                                prevDiff = diff;
                            }
                        }

                        counts.sort(Integer::compareTo);
                        if (counts.isEmpty()) {
                            System.out.println("no adjacent diff for field: " + fieldInfo.getName());
                        } else {
                            System.out.printf(
                                "field name: %s, mean: %d, max= %d\n",
                                fieldInfo.getName(),
                                counts.get(Math.min(counts.size() / 2, counts.size() - 1)),
                                counts.getLast()
                            );
                        }
                    }
                }
            }
        }
    }

    public void testCheckField() throws Exception {
        String fullPath = "/Users/mvg/temp/otel-experiments/data-dir/indices/0Fx-NNq2ROe1QLFoZ4UF4A/0/index";
        try (var directory = FSDirectory.open(Path.of(fullPath))) {
            try (var reader = DirectoryReader.open(directory)) {
                for (var leafReaderContext : reader.leaves()) {
                    var leafReader = leafReaderContext.reader();

                    Set<BytesRef> values = new HashSet<>();
                    var sortedNumericDocValues = leafReader.getSortedSetDocValues("resource.attributes.host.ip");
                    for (int docId = 0; docId < leafReader.maxDoc(); docId++) {
                        if (sortedNumericDocValues.advanceExact(docId)) {
                            for (int i = 0; i < sortedNumericDocValues.docValueCount(); i++) {
                                values.add(BytesRef.deepCopyOf(sortedNumericDocValues.lookupOrd(sortedNumericDocValues.nextOrd())));
                            }
                        }
                    }

                    for (BytesRef value : values) {
                        byte[] bytes = Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length);
                        String ip = NetworkAddress.format(InetAddressPoint.decode(bytes));
                        System.out.println(ip);
                    }
                }
            }
        }
    }

    public void testReadCompounFileSize() throws Exception {
        String fullPath = "/Users/mvg/temp/otel-experiments/data-dir/indices/KhZ-KxSdTLCcevb3pIjeGA/0/index";
        try (var directory = FSDirectory.open(Path.of(fullPath))) {
            try (ChecksumIndexInput entriesStream = directory.openChecksumInput("_9c.cfe")) {
                CodecUtil.checkHeader(entriesStream, "Lucene90CompoundEntries", 0, 0);
                try {
                    CodecUtil.checkIndexHeaderID(entriesStream, new byte[0]);
                } catch (Exception e) {

                }
                CodecUtil.checkIndexHeaderSuffix(entriesStream, "");
//                CodecUtil.checkIndexHeader(
//                        entriesStream,
//                        "Lucene90CompoundEntries",
//                        0,
//                        0,
//                        new byte[0],
//                        "");

                long sum = 0;
                final int numEntries = entriesStream.readVInt();
                Map<String, Long> m = new HashMap<>();
                for (int i = 0; i < numEntries; i++) {
                    final String id = entriesStream.readString();
                    long offset = entriesStream.readLong();
                    long length = entriesStream.readLong();


                    sum += length;
                    m.put(id, length);
                }
                m.entrySet().stream().sorted((o1, o2) -> Long.compare(o2.getValue(), o1.getValue())).forEach(entry -> {
                    System.out.println(entry.getKey() + " / " + ByteSizeValue.ofBytes(entry.getValue()));
                });
                System.out.println("sum = " + ByteSizeValue.ofBytes(sum));
            }
        }
    }

}
