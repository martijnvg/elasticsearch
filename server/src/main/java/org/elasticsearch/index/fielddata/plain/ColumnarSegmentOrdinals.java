/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.columnar.string.DictionaryStringColumnReader;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.StringColumnSource;

import java.io.IOException;
import java.util.Arrays;

/**
 * The ordinals one segment's columnar keyword values take: a number for each distinct value, in term order, which is
 * what {@link org.apache.lucene.index.SortedSetDocValues} promises and what global ordinals are built from.
 *
 * <p>A column with a dictionary and nothing escaped already has them, since its dictionary is in term order and names
 * every value, so {@link #DICTIONARY} costs nothing. Any other column has values no term names, and those are read
 * and ordered once for the segment: the values that escaped a dictionary, by {@link #escapedDictionary}, or every value
 * of a column stored plainly, by {@link #plain}. The result is held in packed arrays and asks the column for bytes only
 * when an ordinal is looked up.
 *
 * <p>Nothing here holds a column. Each method is handed the reader of the doc values it is answering for, so one
 * instance can be cached for the segment and shared by every search of it.
 */
abstract sealed class ColumnarSegmentOrdinals implements Accountable {

    /** The ordinals of a column whose dictionary names every value: its dense ordinals, as they are. */
    static final ColumnarSegmentOrdinals DICTIONARY = new Dictionary();

    /** How many distinct values the segment holds, so its ordinals run from zero up to this. */
    abstract long valueCount(StringColumnReader reader);

    /** The ordinal of the value at {@code valueAddress}, or {@code -1} when the slot is null. */
    abstract long ordinalAt(StringColumnReader reader, long valueAddress) throws IOException;

    /**
     * The ordinal of the one value of the document {@code source} is positioned on, in a column of one slot a document,
     * or {@code -1} when the slot is null. The same answer as {@link #ordinalAt} for the slot, but a column with a
     * dictionary reads it by the document's rank, which is its slot's address, rather than looking that address up
     * first: this is read once for every document a search collects, so the lookup is a cost per document.
     */
    long singleOrdinal(StringColumnSource source, StringColumnReader reader) throws IOException {
        return ordinalAt(reader, source.slotAddress(0));
    }

    /** The value an ordinal stands for. The returned {@link BytesRef} is only valid until the next call. */
    abstract BytesRef lookupOrd(StringColumnReader reader, long ordinal, BytesRef dst) throws IOException;

    /** The ordinals of a column whose dictionary has values escaping it, which are read and ordered among the terms. */
    static ColumnarSegmentOrdinals escapedDictionary(DictionaryStringColumnReader reader) throws IOException {
        return EscapedDictionary.build(reader);
    }

    /** The ordinals of a column stored plainly, whose values are all read and ordered. */
    static ColumnarSegmentOrdinals plain(BinaryDocValues values) throws IOException {
        return Plain.build(values);
    }

    private static final class Dictionary extends ColumnarSegmentOrdinals {

        @Override
        long valueCount(StringColumnReader reader) {
            return reader.dictionarySize();
        }

        @Override
        long ordinalAt(StringColumnReader reader, long valueAddress) throws IOException {
            final int ordinal = ((DictionaryStringColumnReader) reader).denseOrdinalAt(valueAddress);
            if (ordinal == StringColumnSource.NO_VALUE) {
                return -1;
            }
            if (ordinal == StringColumnSource.ESCAPED) {
                throw new IllegalStateException("a value escaped a dictionary that was taken to name every value");
            }
            return ordinal;
        }

        @Override
        long singleOrdinal(StringColumnSource source, StringColumnReader reader) throws IOException {
            final int ordinal = source.denseTermOrdinal();
            if (ordinal >= 0) {
                return ordinal;
            }
            if (ordinal == StringColumnSource.NO_VALUE) {
                return -1;
            }
            throw new IllegalStateException("a value escaped a dictionary that was taken to name every value");
        }

        @Override
        BytesRef lookupOrd(StringColumnReader reader, long ordinal, BytesRef dst) throws IOException {
            return ((DictionaryStringColumnReader) reader).denseTermAt(Math.toIntExact(ordinal), dst);
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }
    }

    /**
     * A dictionary with the values that escaped it ordered among its terms. Both are in term order, so the two are
     * merged in one pass, and a term keeps its place relative to every other term.
     */
    private static final class EscapedDictionary extends ColumnarSegmentOrdinals {

        private final int termCount;
        private final long valueCount;
        /** The ordinal of each dense term ordinal. */
        private final PackedInts.Reader ordinalOfTerm;
        /** The ordinal of each escaped value, by its escape rank. */
        private final PackedInts.Reader ordinalOfEscape;
        /**
         * What each ordinal stands for: a dense term ordinal below {@link #termCount}, otherwise {@link #termCount}
         * plus the index of a distinct escaped value in term order.
         */
        private final PackedInts.Reader sourceOfOrdinal;
        /** The escape rank of one value for each distinct escaped value, in term order, to read its bytes from. */
        private final PackedInts.Reader rankOfEscaped;

        private EscapedDictionary(
            int termCount,
            long valueCount,
            PackedInts.Reader ordinalOfTerm,
            PackedInts.Reader ordinalOfEscape,
            PackedInts.Reader sourceOfOrdinal,
            PackedInts.Reader rankOfEscaped
        ) {
            this.termCount = termCount;
            this.valueCount = valueCount;
            this.ordinalOfTerm = ordinalOfTerm;
            this.ordinalOfEscape = ordinalOfEscape;
            this.sourceOfOrdinal = sourceOfOrdinal;
            this.rankOfEscaped = rankOfEscaped;
        }

        static EscapedDictionary build(DictionaryStringColumnReader reader) throws IOException {
            final int termCount = reader.dictionarySize();
            final int escapeCount = Math.toIntExact(reader.escapeCount());
            final BytesRefHash escaped = new BytesRefHash();
            try {
                final int[] idOfRank = new int[escapeCount];
                int[] firstRankOfId = new int[0];
                final BytesRef scratch = new BytesRef();
                for (int rank = 0; rank < escapeCount; rank++) {
                    int id = escaped.add(reader.escapeAt(rank, scratch));
                    if (id < 0) {
                        id = -1 - id;
                    } else {
                        firstRankOfId = ArrayUtil.grow(firstRankOfId, id + 1);
                        firstRankOfId[id] = rank;
                    }
                    idOfRank[rank] = id;
                }
                final int distinct = escaped.size();
                final int[] sortedIds = escaped.sort();

                final int maxValueCount = termCount + distinct;
                final int ordinalBits = PackedInts.bitsRequired(Math.max(0, maxValueCount - 1));
                final PackedInts.Mutable ordinalOfTerm = PackedInts.getMutable(termCount, ordinalBits, PackedInts.COMPACT);
                final PackedInts.Mutable sourceOfOrdinal = PackedInts.getMutable(maxValueCount, ordinalBits, PackedInts.COMPACT);
                final PackedInts.Mutable rankOfEscaped = PackedInts.getMutable(
                    distinct,
                    PackedInts.bitsRequired(Math.max(0, escapeCount - 1)),
                    PackedInts.COMPACT
                );
                final int[] ordinalOfId = new int[distinct];

                final BytesRef term = new BytesRef();
                final BytesRef value = new BytesRef();
                int t = 0;
                int e = 0;
                int ordinal = 0;
                while (t < termCount || e < distinct) {
                    final int cmp;
                    if (t == termCount) {
                        cmp = 1;
                    } else if (e == distinct) {
                        cmp = -1;
                    } else {
                        reader.denseTermAt(t, term);
                        escaped.get(sortedIds[e], value);
                        cmp = term.compareTo(value);
                    }
                    if (cmp <= 0) {
                        ordinalOfTerm.set(t, ordinal);
                        sourceOfOrdinal.set(ordinal, t);
                        t++;
                    }
                    if (cmp >= 0) {
                        // NOTE: an escaped value is one the dictionary has no term for, so the two never tie. Should they,
                        // the value takes the term's ordinal rather than one of its own, which keeps the ordinals distinct.
                        ordinalOfId[sortedIds[e]] = ordinal;
                        if (cmp > 0) {
                            sourceOfOrdinal.set(ordinal, termCount + e);
                        }
                        rankOfEscaped.set(e, firstRankOfId[sortedIds[e]]);
                        e++;
                    }
                    ordinal++;
                }

                final PackedInts.Mutable ordinalOfEscape = PackedInts.getMutable(escapeCount, ordinalBits, PackedInts.COMPACT);
                for (int rank = 0; rank < escapeCount; rank++) {
                    ordinalOfEscape.set(rank, ordinalOfId[idOfRank[rank]]);
                }
                return new EscapedDictionary(termCount, ordinal, ordinalOfTerm, ordinalOfEscape, sourceOfOrdinal, rankOfEscaped);
            } finally {
                escaped.close();
            }
        }

        @Override
        long valueCount(StringColumnReader reader) {
            return valueCount;
        }

        @Override
        long ordinalAt(StringColumnReader reader, long valueAddress) throws IOException {
            final DictionaryStringColumnReader dictionary = (DictionaryStringColumnReader) reader;
            final int dense = dictionary.denseOrdinalAt(valueAddress);
            if (dense >= 0) {
                return ordinalOfTerm.get(dense);
            }
            if (dense == StringColumnSource.NO_VALUE) {
                return -1;
            }
            assert dense == StringColumnSource.ESCAPED : dense;
            return ordinalOfEscape.get(Math.toIntExact(dictionary.escapeRank(valueAddress)));
        }

        @Override
        long singleOrdinal(StringColumnSource source, StringColumnReader reader) throws IOException {
            final int dense = source.denseTermOrdinal();
            if (dense >= 0) {
                return ordinalOfTerm.get(dense);
            }
            if (dense == StringColumnSource.NO_VALUE) {
                return -1;
            }
            assert dense == StringColumnSource.ESCAPED : dense;
            // Rare enough, a value no term names, that looking up where it is costs nothing worth saving.
            return ordinalOfEscape.get(Math.toIntExact(((DictionaryStringColumnReader) reader).escapeRank(source.slotAddress(0))));
        }

        @Override
        BytesRef lookupOrd(StringColumnReader reader, long ordinal, BytesRef dst) throws IOException {
            final DictionaryStringColumnReader dictionary = (DictionaryStringColumnReader) reader;
            final long source = sourceOfOrdinal.get(Math.toIntExact(ordinal));
            if (source < termCount) {
                return dictionary.denseTermAt((int) source, dst);
            }
            return dictionary.escapeAt(rankOfEscaped.get((int) (source - termCount)), dst);
        }

        @Override
        public long ramBytesUsed() {
            return ordinalOfTerm.ramBytesUsed() + ordinalOfEscape.ramBytesUsed() + sourceOfOrdinal.ramBytesUsed() + rankOfEscaped
                .ramBytesUsed();
        }
    }

    /** A column stored plainly: every value read once and ordered, and an ordinal kept for each slot. */
    private static final class Plain extends ColumnarSegmentOrdinals {

        /** One more than the ordinal of each slot, by value address, so that zero stands for a null slot. */
        private final PackedInts.Reader ordinalOfAddress;
        /** The value address of one slot holding each ordinal's value, to read its bytes from. */
        private final PackedInts.Reader addressOfOrdinal;

        private Plain(PackedInts.Reader ordinalOfAddress, PackedInts.Reader addressOfOrdinal) {
            this.ordinalOfAddress = ordinalOfAddress;
            this.addressOfOrdinal = addressOfOrdinal;
        }

        static Plain build(BinaryDocValues values) throws IOException {
            final StringColumnSource source = (StringColumnSource) values;
            final StringColumnReader reader = source.reader();
            final int addressCount = Math.toIntExact(reader.numValues());
            final int[] idOfAddress = new int[addressCount];
            Arrays.fill(idOfAddress, -1);
            final BytesRefHash hash = new BytesRefHash();
            try {
                int[] addressOfId = new int[0];
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    final int slots = source.slotCount();
                    for (int slot = 0; slot < slots; slot++) {
                        final int address = Math.toIntExact(source.slotAddress(slot));
                        final BytesRef value = reader.valueAt(address);
                        if (value == null) {
                            continue;
                        }
                        int id = hash.add(value);
                        if (id < 0) {
                            id = -1 - id;
                        } else {
                            addressOfId = ArrayUtil.grow(addressOfId, id + 1);
                            addressOfId[id] = address;
                        }
                        idOfAddress[address] = id;
                    }
                }
                final int distinct = hash.size();
                final int[] sortedIds = hash.sort();
                final int[] ordinalOfId = new int[distinct];
                final PackedInts.Mutable addressOfOrdinal = PackedInts.getMutable(
                    distinct,
                    PackedInts.bitsRequired(Math.max(0, addressCount - 1)),
                    PackedInts.COMPACT
                );
                for (int ordinal = 0; ordinal < distinct; ordinal++) {
                    ordinalOfId[sortedIds[ordinal]] = ordinal;
                    addressOfOrdinal.set(ordinal, addressOfId[sortedIds[ordinal]]);
                }
                final PackedInts.Mutable ordinalOfAddress = PackedInts.getMutable(
                    addressCount,
                    PackedInts.bitsRequired(distinct),
                    PackedInts.COMPACT
                );
                for (int address = 0; address < addressCount; address++) {
                    final int id = idOfAddress[address];
                    if (id >= 0) {
                        ordinalOfAddress.set(address, ordinalOfId[id] + 1L);
                    }
                }
                return new Plain(ordinalOfAddress, addressOfOrdinal);
            } finally {
                hash.close();
            }
        }

        @Override
        long valueCount(StringColumnReader reader) {
            return addressOfOrdinal.size();
        }

        @Override
        long ordinalAt(StringColumnReader reader, long valueAddress) {
            return ordinalOfAddress.get(Math.toIntExact(valueAddress)) - 1;
        }

        @Override
        BytesRef lookupOrd(StringColumnReader reader, long ordinal, BytesRef dst) throws IOException {
            return reader.valueAt(addressOfOrdinal.get(Math.toIntExact(ordinal)));
        }

        @Override
        public long ramBytesUsed() {
            return ordinalOfAddress.ramBytesUsed() + addressOfOrdinal.ramBytesUsed();
        }
    }
}
