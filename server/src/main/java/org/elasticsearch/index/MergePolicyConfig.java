/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.SuppressForbidden;

/**
 * A shard in elasticsearch is a Lucene index, and a Lucene index is broken
 * down into segments. Segments are internal storage elements in the index
 * where the index data is stored, and are immutable up to delete markers.
 * Segments are, periodically, merged into larger segments to keep the
 * index size at bay and expunge deletes.
 *
 * <p>
 * Merges select segments of approximately equal size, subject to an allowed
 * number of segments per tier. The merge policy is able to merge
 * non-adjacent segments, and separates how many segments are merged at once from how many
 * segments are allowed per tier. It also does not over-merge (i.e., cascade merges).
 *
 * <p>
 * All merge policy settings are <b>dynamic</b> and can be updated on a live index.
 * The merge policy has the following settings:
 *
 * <ul>
 * <li><code>index.merge.policy.expunge_deletes_allowed</code>:
 *
 *     When forceMergeDeletes is called, we only merge away a segment if its delete
 *     percentage is over this threshold. Default is <code>10</code>.
 *
 * <li><code>index.merge.policy.floor_segment</code>:
 *
 *     Segments smaller than this are "rounded up" to this size, i.e. treated as
 *     equal (floor) size for merge selection. This is to prevent frequent
 *     flushing of tiny segments, thus preventing a long tail in the index. Default
 *     is <code>2mb</code>.
 *
 * <li><code>index.merge.policy.max_merge_at_once</code>:
 *
 *     Maximum number of segments to be merged at a time during "normal" merging.
 *     Default is <code>10</code>.
 *
 * <li><code>index.merge.policy.max_merged_segment</code>:
 *
 *     Maximum sized segment to produce during normal merging (not explicit
 *     force merge). This setting is approximate: the estimate of the merged
 *     segment size is made by summing sizes of to-be-merged segments
 *     (compensating for percent deleted docs). Default is <code>5gb</code>.
 *
 * <li><code>index.merge.policy.segments_per_tier</code>:
 *
 *     Sets the allowed number of segments per tier. Smaller values mean more
 *     merging but fewer segments. Default is <code>10</code>. Note, this value needs to be
 *     &gt;= than the <code>max_merge_at_once</code> otherwise you'll force too many merges to
 *     occur.
 *
 * <li><code>index.merge.policy.deletes_pct_allowed</code>:
 *
 *     Controls the maximum percentage of deleted documents that is tolerated in
 *     the index. Lower values make the index more space efficient at the
 *     expense of increased CPU and I/O activity. Values must be between <code>20</code> and
 *     <code>50</code>. Default value is <code>33</code>.
 * </ul>
 *
 * <p>
 * For normal merging, the policy first computes a "budget" of how many
 * segments are allowed to be in the index. If the index is over-budget,
 * then the policy sorts segments by decreasing size (proportionally considering percent
 * deletes), and then finds the least-cost merge. Merge cost is measured by
 * a combination of the "skew" of the merge (size of largest seg divided by
 * smallest seg), total merge size and pct deletes reclaimed, so that
 * merges with lower skew, smaller size and those reclaiming more deletes,
 * are favored.
 *
 * <p>
 * If a merge will produce a segment that's larger than
 * <code>max_merged_segment</code> then the policy will merge fewer segments (down to
 * 1 at once, if that one has deletions) to keep the segment size under
 * budget.
 *
 * <p>
 * Note, this can mean that for large shards that holds many gigabytes of
 * data, the default of <code>max_merged_segment</code> (<code>5gb</code>) can cause for many
 * segments to be in an index, and causing searches to be slower. Use the
 * indices segments API to see the segments that an index has, and
 * possibly either increase the <code>max_merged_segment</code> or issue an optimize
 * call for the index (try and aim to issue it on a low traffic time).
 */

public final class MergePolicyConfig {
    private final TieredMergePolicy tieredMergePolicy = new TieredMergePolicy();
    private final LogByteSizeMergePolicy logByteSizeMergePolicy = new LogByteSizeMergePolicy();
    private final Logger logger;
    private final boolean mergesEnabled;
    private volatile Type mergePolicyType;

    public static final double DEFAULT_EXPUNGE_DELETES_ALLOWED = 10d;
    public static final ByteSizeValue DEFAULT_FLOOR_SEGMENT = new ByteSizeValue(2, ByteSizeUnit.MB);
    public static final int DEFAULT_MAX_MERGE_AT_ONCE = 10;
    public static final ByteSizeValue DEFAULT_MAX_MERGED_SEGMENT = new ByteSizeValue(5, ByteSizeUnit.GB);
    public static final double DEFAULT_SEGMENTS_PER_TIER = 10.0d;
    /**
     * A default value for {@link LogByteSizeMergePolicy}'s merge factor: 16. This default value differs from the Lucene default of 10 in
     * order to account for the fact that Elasticsearch uses {@link LogByteSizeMergePolicy} for time-based data, where it usually makes
     * sense to merge data less aggressively, and because {@link LogByteSizeMergePolicy} merges segments more aggressively than
     * {@link TieredMergePolicy} for the same number of segments per tier / merge factor because {@link TieredMergePolicy} makes decisions
     * at the whole index level, while {@link LogByteSizeMergePolicy} makes decisions on a per-tier basis.
     */
    public static final int DEFAULT_MERGE_FACTOR = 16;
    public static final double DEFAULT_DELETES_PCT_ALLOWED = 33.0d;
    private static final String INDEX_COMPOUND_FORMAT_SETTING_KEY = "index.compound_format";
    public static final Setting<CompoundFileThreshold> INDEX_COMPOUND_FORMAT_SETTING = new Setting<>(
        INDEX_COMPOUND_FORMAT_SETTING_KEY,
        "1gb",
        MergePolicyConfig::parseCompoundFormat,
        Property.Dynamic,
        Property.IndexScope
    );

    public enum Type {
        UNSET {
            @Override
            MergePolicy getMergePolicy(MergePolicyConfig config, boolean isTimeBasedIndex) {
                if (isTimeBasedIndex) {
                    // With time-based data, it's important that the merge policy only merges adjacent segments, so that segments end up
                    // with non-overlapping time ranges if data gets indexed in order. This makes queries more efficient, as range filters
                    // on the timestamp are more likely to either fully match a segment or not match it at all, which Lucene handles more
                    // efficiently than a partially matching segment. This also plays nicely with the fact that recent data is more heavily
                    // queried than older data, so some segments are more likely to not get touched at all by queries if they don't
                    // intersect with the query's range.

                    // The downside of only doing adjacent merges is that it may result in slightly less efficient merging if there is a lot
                    // of variance in the size of flushes. Allowing merges of non-adjacent segments also makes it possible to reclaim
                    // deletes a bit more efficiently by merging together segments that have the most deletes, even though they might not be
                    // adjacent. But overall, the benefits of only doing adjacent merging exceed the downsides for time-based data.

                    // LogByteSizeMergePolicy is similar to TieredMergePolicy, as it also tries to organize segments into tiers of
                    // exponential sizes. The main difference is that it never merges non-adjacent segments, which is an interesting
                    // property for time-based data as described above.

                    return config.logByteSizeMergePolicy;
                } else {
                    return config.tieredMergePolicy;
                }
            }
        },
        TIERED {
            @Override
            MergePolicy getMergePolicy(MergePolicyConfig config, boolean isTimeBasedIndex) {
                return config.tieredMergePolicy;
            }
        },
        LOG_BYTE_SIZE {
            @Override
            MergePolicy getMergePolicy(MergePolicyConfig config, boolean isTimeBasedIndex) {
                return config.logByteSizeMergePolicy;
            }
        };

        abstract MergePolicy getMergePolicy(MergePolicyConfig config, boolean isTimeSeries);
    }

    public static final Setting<Type> INDEX_MERGE_POLICY_TYPE_SETTING = Setting.enumSetting(
        Type.class,
        "index.merge.policy.type",
        Type.UNSET,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Double> INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING = Setting.doubleSetting(
        "index.merge.policy.expunge_deletes_allowed",
        DEFAULT_EXPUNGE_DELETES_ALLOWED,
        0.0d,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<ByteSizeValue> INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING = Setting.byteSizeSetting(
        "index.merge.policy.floor_segment",
        DEFAULT_FLOOR_SEGMENT,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Integer> INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING = Setting.intSetting(
        "index.merge.policy.max_merge_at_once",
        DEFAULT_MAX_MERGE_AT_ONCE,
        2,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Integer> INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT_SETTING = Setting.intSetting(
        "index.merge.policy.max_merge_at_once_explicit",
        30,
        2,
        Property.Deprecated, // When removing in 9.0 follow the approach of IndexSettingDeprecatedInV7AndRemovedInV8
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<ByteSizeValue> INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING = Setting.byteSizeSetting(
        "index.merge.policy.max_merged_segment",
        DEFAULT_MAX_MERGED_SEGMENT,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Double> INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING = Setting.doubleSetting(
        "index.merge.policy.segments_per_tier",
        DEFAULT_SEGMENTS_PER_TIER,
        2.0d,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Integer> INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING = Setting.intSetting(
        "index.merge.policy.merge_factor",
        DEFAULT_MERGE_FACTOR,
        2,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Double> INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING = Setting.doubleSetting(
        "index.merge.policy.deletes_pct_allowed",
        DEFAULT_DELETES_PCT_ALLOWED,
        20.0d,
        50.0d,
        Property.Dynamic,
        Property.IndexScope
    );
    // don't convert to Setting<> and register... we only set this in tests and register via a plugin
    public static final String INDEX_MERGE_ENABLED = "index.merge.enabled";

    MergePolicyConfig(Logger logger, IndexSettings indexSettings) {
        this.logger = logger;
        Type mergePolicyType = indexSettings.getValue(INDEX_MERGE_POLICY_TYPE_SETTING);
        double forceMergeDeletesPctAllowed = indexSettings.getValue(INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING); // percentage
        ByteSizeValue floorSegment = indexSettings.getValue(INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING);
        int maxMergeAtOnce = indexSettings.getValue(INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING);
        // TODO is this really a good default number for max_merge_segment, what happens for large indices,
        // won't they end up with many segments?
        ByteSizeValue maxMergedSegment = indexSettings.getValue(INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING);
        double segmentsPerTier = indexSettings.getValue(INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING);
        int mergeFactor = indexSettings.getValue(INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING);
        double deletesPctAllowed = indexSettings.getValue(INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING);
        this.mergesEnabled = indexSettings.getSettings().getAsBoolean(INDEX_MERGE_ENABLED, true);
        if (mergesEnabled == false) {
            logger.warn(
                "[{}] is set to false, this should only be used in tests and can cause serious problems in production" + " environments",
                INDEX_MERGE_ENABLED
            );
        }
        maxMergeAtOnce = adjustMaxMergeAtOnceIfNeeded(maxMergeAtOnce, segmentsPerTier);
        setMergePolicyType(mergePolicyType);
        setCompoundFormatThreshold(indexSettings.getValue(INDEX_COMPOUND_FORMAT_SETTING));
        setExpungeDeletesAllowed(forceMergeDeletesPctAllowed);
        setFloorSegmentSetting(floorSegment);
        setMaxMergesAtOnce(maxMergeAtOnce);
        setMaxMergedSegment(maxMergedSegment);
        setSegmentsPerTier(segmentsPerTier);
        setMergeFactor(mergeFactor);
        setDeletesPctAllowed(deletesPctAllowed);
        logger.trace(
            "using merge policy with expunge_deletes_allowed[{}], floor_segment[{}],"
                + " max_merge_at_once[{}], max_merged_segment[{}], segments_per_tier[{}],"
                + " deletes_pct_allowed[{}]",
            forceMergeDeletesPctAllowed,
            floorSegment,
            maxMergeAtOnce,
            maxMergedSegment,
            segmentsPerTier,
            deletesPctAllowed
        );
    }

    void setMergePolicyType(Type type) {
        this.mergePolicyType = type;
    }

    void setSegmentsPerTier(double segmentsPerTier) {
        tieredMergePolicy.setSegmentsPerTier(segmentsPerTier);
        // LogByteSizeMergePolicy ignores this parameter, it always tries to have between 1 and merge_factor - 1 segments per tier.
    }

    void setMergeFactor(int mergeFactor) {
        // TieredMergePolicy ignores this setting, it configures a number of segments per tier instead, which has different semantics.
        logByteSizeMergePolicy.setMergeFactor(mergeFactor);
    }

    void setMaxMergedSegment(ByteSizeValue maxMergedSegment) {
        tieredMergePolicy.setMaxMergedSegmentMB(maxMergedSegment.getMbFrac());
        logByteSizeMergePolicy.setMaxMergeMB(maxMergedSegment.getMbFrac());
    }

    void setMaxMergesAtOnce(int maxMergeAtOnce) {
        tieredMergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
        // LogByteSizeMergePolicy ignores this parameter, it always merges merge_factor segments at once.
    }

    void setFloorSegmentSetting(ByteSizeValue floorSegementSetting) {
        tieredMergePolicy.setFloorSegmentMB(floorSegementSetting.getMbFrac());
        logByteSizeMergePolicy.setMinMergeMB(floorSegementSetting.getMbFrac());
    }

    void setExpungeDeletesAllowed(Double value) {
        tieredMergePolicy.setForceMergeDeletesPctAllowed(value);
        // LogByteSizeMergePolicy doesn't have a similar configuration option
    }

    void setCompoundFormatThreshold(CompoundFileThreshold compoundFileThreshold) {
        compoundFileThreshold.configure(tieredMergePolicy);
        compoundFileThreshold.configure(logByteSizeMergePolicy);
    }

    void setDeletesPctAllowed(Double deletesPctAllowed) {
        tieredMergePolicy.setDeletesPctAllowed(deletesPctAllowed);
        // LogByteSizeMergePolicy doesn't have a similar configuration option
    }

    private int adjustMaxMergeAtOnceIfNeeded(int maxMergeAtOnce, double segmentsPerTier) {
        // fixing maxMergeAtOnce, see TieredMergePolicy#setMaxMergeAtOnce
        if (segmentsPerTier < maxMergeAtOnce) {
            int newMaxMergeAtOnce = (int) segmentsPerTier;
            // max merge at once should be at least 2
            if (newMaxMergeAtOnce <= 1) {
                newMaxMergeAtOnce = 2;
            }
            logger.debug(
                "changing max_merge_at_once from [{}] to [{}] because segments_per_tier [{}] has to be higher or " + "equal to it",
                maxMergeAtOnce,
                newMaxMergeAtOnce,
                segmentsPerTier
            );
            maxMergeAtOnce = newMaxMergeAtOnce;
        }
        return maxMergeAtOnce;
    }

    @SuppressForbidden(reason = "we always use an appropriate merge scheduler alongside this policy so NoMergePolic#INSTANCE is ok")
    MergePolicy getMergePolicy(boolean isTimeBasedIndex) {
        if (mergesEnabled == false) {
            return NoMergePolicy.INSTANCE;
        }
        return mergePolicyType.getMergePolicy(this, isTimeBasedIndex);
    }

    private static CompoundFileThreshold parseCompoundFormat(String noCFSRatio) {
        noCFSRatio = noCFSRatio.trim();
        if (noCFSRatio.equalsIgnoreCase("true")) {
            return new CompoundFileThreshold(1.0d);
        } else if (noCFSRatio.equalsIgnoreCase("false")) {
            return new CompoundFileThreshold(0.0d);
        } else {
            try {
                try {
                    return new CompoundFileThreshold(Double.parseDouble(noCFSRatio));
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException(
                        "index.compound_format must be a boolean, a non-negative byte size or a ratio in the interval [0..1] but was: ["
                            + noCFSRatio
                            + "]",
                        ex
                    );
                }
            } catch (IllegalArgumentException e) {
                try {
                    return new CompoundFileThreshold(ByteSizeValue.parseBytesSizeValue(noCFSRatio, INDEX_COMPOUND_FORMAT_SETTING_KEY));
                } catch (RuntimeException e2) {
                    e.addSuppressed(e2);
                }
                throw e;
            }
        }
    }

    public static class CompoundFileThreshold {
        private Double noCFSRatio;
        private ByteSizeValue noCFSSize;

        private CompoundFileThreshold(double noCFSRatio) {
            if (noCFSRatio < 0.0 || noCFSRatio > 1.0) {
                throw new IllegalArgumentException(
                    "index.compound_format must be a boolean, a non-negative byte size or a ratio in the interval [0..1] but was: ["
                        + noCFSRatio
                        + "]"
                );
            }
            this.noCFSRatio = noCFSRatio;
            this.noCFSSize = null;
        }

        private CompoundFileThreshold(ByteSizeValue noCFSSize) {
            if (noCFSSize.getBytes() < 0) {
                throw new IllegalArgumentException(
                    "index.compound_format must be a boolean, a non-negative byte size or a ratio in the interval [0..1] but was: ["
                        + noCFSSize
                        + "]"
                );
            }
            this.noCFSRatio = null;
            this.noCFSSize = noCFSSize;
        }

        void configure(MergePolicy mergePolicy) {
            if (noCFSRatio != null) {
                assert noCFSSize == null;
                mergePolicy.setNoCFSRatio(noCFSRatio);
                mergePolicy.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
            } else {
                mergePolicy.setNoCFSRatio(1.0);
                mergePolicy.setMaxCFSSegmentSizeMB(noCFSSize.getMbFrac());
            }
        }

        @Override
        public String toString() {
            if (noCFSRatio != null) {
                return "max CFS ratio: " + noCFSRatio;
            } else {
                return "max CFS size: " + noCFSSize;
            }
        }
    }
}
