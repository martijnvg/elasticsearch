/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.aggregations;

import org.elasticsearch.core.CheckedRunnable;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runs a benchmark as a plain program rather than through JMH: untimed warmup rounds, then measured rounds, each timed
 * with {@link System#nanoTime} and rounded to whole milliseconds. This suits benchmarks whose setup is expensive, such as
 * indexing ten million documents, which JMH would redo for every fork and parameter combination, and whose rounds are
 * long enough that the latency of each one, as a search sees it, says more than an average does.
 *
 * <p>Parameters are given as {@code name=value[,value...]} arguments; a benchmark loops over the values of each one.
 * {@code warmup} and {@code iterations} set how many rounds are run untimed and timed.
 *
 * <p>{@code profile=<directory>} also draws a {@link FlameGraph} of each configuration into that directory, from rounds
 * run for {@code profileSeconds} after the timed ones, so the profiler doesn't slow down the rounds that are timed. A
 * relative directory is resolved against the directory the benchmark runs in, which for {@code runMain} is
 * {@code benchmarks}.
 */
final class LatencyHarness {
    private final Map<String, List<String>> params = new LinkedHashMap<>();

    LatencyHarness(String[] args, Map<String, String> defaults) {
        params.put("warmup", List.of("10"));
        params.put("iterations", List.of("30"));
        params.put("profile", List.of(""));
        params.put("profileSeconds", List.of("5"));
        defaults.forEach((name, values) -> params.put(name, List.of(values.split(","))));
        for (String arg : args) {
            final int eq = arg.indexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException("expected name=value[,value...] but got [" + arg + "]");
            }
            final String name = arg.substring(0, eq);
            if (params.containsKey(name) == false) {
                throw new IllegalArgumentException("unknown parameter [" + name + "], expected one of " + params.keySet());
            }
            params.put(name, List.of(arg.substring(eq + 1).split(",")));
        }
        System.out.println("parameters " + params);
    }

    /** Every value given for a parameter. */
    List<String> values(String name) {
        final List<String> values = params.get(name);
        if (values == null) {
            throw new IllegalArgumentException("unknown parameter [" + name + "]");
        }
        return values;
    }

    /** The one value of a parameter that doesn't take several. */
    int intValue(String name) {
        final List<String> values = values(name);
        if (values.size() != 1) {
            throw new IllegalArgumentException("[" + name + "] takes one value but got " + values);
        }
        return Integer.parseInt(values.get(0));
    }

    /**
     * Runs the warmup rounds, then times each measured round, in milliseconds. With {@code profile} set, it then draws
     * a flame graph of further rounds, named after {@code configuration}.
     */
    long[] measure(String configuration, CheckedRunnable<IOException> round) throws IOException {
        for (int i = 0, warmup = intValue("warmup"); i < warmup; i++) {
            round.run();
        }
        final long[] millis = new long[intValue("iterations")];
        for (int i = 0; i < millis.length; i++) {
            final long start = System.nanoTime();
            round.run();
            millis[i] = Math.round((System.nanoTime() - start) / 1_000_000.0);
        }
        final String profile = values("profile").getFirst();
        if (profile.isEmpty() == false) {
            FlameGraph.profile(Path.of(profile), configuration, round, Duration.ofSeconds(intValue("profileSeconds")));
        }
        return millis;
    }

    /** Prints a configuration's latencies: their spread, then every round in the order it ran. */
    static void report(String configuration, long[] millis) {
        final long[] sorted = millis.clone();
        Arrays.sort(sorted);
        System.out.printf(
            "%s: min=%d median=%d p90=%d max=%d mean=%d ms over %d rounds %s%n",
            configuration,
            sorted[0],
            sorted[sorted.length / 2],
            sorted[(int) Math.ceil(sorted.length * 0.9) - 1],
            sorted[sorted.length - 1],
            Math.round(Arrays.stream(sorted).average().orElse(0)),
            sorted.length,
            Arrays.toString(millis)
        );
    }
}
