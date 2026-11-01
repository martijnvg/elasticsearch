/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.aggregations;

import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingFile;

import org.elasticsearch.core.CheckedRunnable;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Profiles a benchmark round with the JDK's own flight recorder and draws a flame graph of it, so a configuration that
 * is slower than another can be looked into without attaching a profiler by hand or installing one. The rounds are run
 * for a fixed time rather than a fixed count, so a fast query gets as many samples as a slow one.
 *
 * <p>Each configuration leaves three files: the recording ({@code .jfr}, for JDK Mission Control or async-profiler's
 * {@code jfrconv}), its stacks in the collapsed format that most flame graph tools read ({@code .collapsed}), and a
 * flame graph that opens in a browser on its own ({@code .html}). Only the thread running the rounds is kept, so the
 * graph shows the query, not the JVM's own threads. Stacks are as deep as the JVM keeps them: {@code runMain} raises
 * the flight recorder's limit of 64 frames, and deeper stacks are put under {@code [truncated]}.
 */
final class FlameGraph {
    private static final Duration SAMPLE_PERIOD = Duration.ofMillis(1);

    private FlameGraph() {}

    /**
     * Runs {@code round} over and over for {@code duration} while sampling it, then writes the recording, its stacks
     * and a flame graph of them into {@code directory}, named after the configuration.
     */
    static void profile(Path directory, String configuration, CheckedRunnable<IOException> round, Duration duration) throws IOException {
        Files.createDirectories(directory);
        final String name = configuration.replaceAll("[^A-Za-z0-9._-]+", "_");
        final Path jfr = directory.resolve(name + ".jfr");
        final long thread = Thread.currentThread().threadId();
        int rounds = 0;
        final long start = System.nanoTime();
        try (Recording recording = new Recording()) {
            recording.enable("jdk.ExecutionSample").withPeriod(SAMPLE_PERIOD);
            recording.start();
            final long end = start + duration.toNanos();
            do {
                round.run();
                rounds++;
            } while (System.nanoTime() < end);
            recording.stop();
            recording.dump(jfr);
        }
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        final Node root = new Node("all");
        for (RecordedEvent event : RecordingFile.readAllEvents(jfr)) {
            final RecordedThread sampled = event.getThread("sampledThread");
            final RecordedStackTrace stack = event.getStackTrace();
            if (sampled == null || sampled.getJavaThreadId() != thread || stack == null) {
                continue;
            }
            Node node = root;
            node.value++;
            if (stack.isTruncated()) {
                node = node.child("[truncated]");
                node.value++;
            }
            final List<RecordedFrame> frames = stack.getFrames();
            for (int i = frames.size() - 1; i >= 0; i--) {
                node = node.child(frames.get(i).getMethod().getType().getName() + "." + frames.get(i).getMethod().getName());
                node.value++;
            }
        }

        final Path collapsed = directory.resolve(name + ".collapsed");
        try (Writer writer = Files.newBufferedWriter(collapsed, StandardCharsets.UTF_8)) {
            for (Node child : root.children.values()) {
                child.writeCollapsed(child.name, writer);
            }
        }
        final Path html = directory.resolve(name + ".html");
        final String subtitle = root.value
            + " samples, one every "
            + SAMPLE_PERIOD.toMillis()
            + " ms of the benchmark thread, over "
            + rounds
            + " rounds in "
            + millis
            + " ms";
        Files.writeString(html, html(configuration, subtitle, root), StandardCharsets.UTF_8);
        System.out.println(configuration + ": profiled " + subtitle + ", flame graph at " + html.toAbsolutePath());
    }

    /** A frame in the call tree, with how many samples were in it or in what it called. */
    private static final class Node {
        private final String name;
        private final Map<String, Node> children = new TreeMap<>();
        private long value;

        Node(String name) {
            this.name = name;
        }

        Node child(String name) {
            return children.computeIfAbsent(name, Node::new);
        }

        /** Writes a line per stack that samples ended in, as {@code frame;frame;frame samples}, root first. */
        void writeCollapsed(String stack, Writer writer) throws IOException {
            long self = value;
            for (Node child : children.values()) {
                self -= child.value;
                child.writeCollapsed(stack + ";" + child.name, writer);
            }
            if (self > 0) {
                writer.write(stack + " " + self + "\n");
            }
        }

        /** Writes the tree as {@code [name, samples, [children]]}, with names as indexes into a table so the page stays small. */
        void writeJson(Map<String, Integer> names, StringBuilder json) {
            json.append('[').append(names.computeIfAbsent(name, n -> names.size())).append(',').append(value).append(",[");
            boolean first = true;
            for (Node child : children.values()) {
                if (first == false) {
                    json.append(',');
                }
                first = false;
                child.writeJson(names, json);
            }
            json.append("]]");
        }
    }

    private static String html(String title, String subtitle, Node root) {
        final Map<String, Integer> names = new HashMap<>();
        final StringBuilder tree = new StringBuilder();
        root.writeJson(names, tree);
        final String[] table = new String[names.size()];
        names.forEach((name, index) -> table[index] = name);
        final StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < table.length; i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append(jsonString(table[i]));
        }
        json.append(']');
        return TEMPLATE.replace("$TITLE", escapeHtml(title))
            .replace("$SUBTITLE", escapeHtml(subtitle))
            .replace("$NAMES", json)
            .replace("$TREE", tree);
    }

    private static String jsonString(String value) {
        final StringBuilder json = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            switch (c) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                // Keeps a name from closing the script element it is in.
                case '<' -> json.append("\\u003c");
                default -> {
                    if (c < 0x20) {
                        json.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        json.append(c);
                    }
                }
            }
        }
        return json.append('"').toString();
    }

    private static String escapeHtml(String value) {
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    /**
     * The page: callers on top and what they called below them, each frame as wide as its share of the samples. It is
     * drawn on a canvas, since a query's graph has tens of thousands of frames.
     */
    private static final String TEMPLATE =
        """
            <!doctype html>
            <html lang="en">
            <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>$TITLE</title>
            <style>
              :root { --bg: #ffffff; --fg: #1f2328; --muted: #59636e; --line: #d1d9e0; }
              @media (prefers-color-scheme: dark) { :root { --bg: #0d1117; --fg: #e6edf3; --muted: #9198a1; --line: #3d444d; } }
              body { margin: 0; background: var(--bg); color: var(--fg); font: 12px ui-monospace, Menlo, monospace; }
              header { position: sticky; top: 0; z-index: 1; background: var(--bg); border-bottom: 1px solid var(--line);
                padding: 8px 16px; }
              h1 { font-size: 13px; margin: 0 0 2px; overflow-wrap: anywhere; }
              .muted { color: var(--muted); }
              .controls { margin-top: 6px; display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }
              input { font: inherit; width: min(28em, 100%); padding: 2px 4px; background: var(--bg); color: var(--fg);
                border: 1px solid var(--line); }
              #status { margin-top: 4px; min-height: 1.3em; white-space: pre-wrap; overflow-wrap: anywhere; }
              canvas { display: block; margin: 0 16px; }
            </style>
            </head>
            <body>
            <header>
              <h1>$TITLE</h1>
              <div class="muted">$SUBTITLE</div>
              <div class="controls">
                <input id="search" placeholder="Search frames (regex), e.g. RemapGlobalOrds" aria-label="Search frames">
                <span id="matched" class="muted"></span>
              </div>
              <div id="status" class="muted">Hover a frame for its samples; click it to zoom in, click a frame above it to zoom out.</div>
            </header>
            <canvas id="graph"></canvas>
            <script>
            const names = $NAMES;
            const ROW = 16;
            let depth = 0;
            function build(raw, parent, level) {
              const node = { name: names[raw[0]], value: raw[1], parent: parent, level: level, children: [] };
              depth = Math.max(depth, level);
              for (const child of raw[2]) node.children.push(build(child, node, level + 1));
              return node;
            }
            const root = build($TREE, null, 0);
            const canvas = document.getElementById('graph');
            const context = canvas.getContext('2d');
            const status = document.getElementById('status');
            let zoomed = root;
            let pattern = null;
            let placed = [];

            function shortName(name) {
              const method = name.lastIndexOf('.');
              const type = method > 0 ? name.lastIndexOf('.', method - 1) : -1;
              return type < 0 ? name : name.substring(type + 1);
            }
            function hash(text) {
              let h = 0;
              for (let i = 0; i < text.length; i++) h = (h * 31 + text.charCodeAt(i)) | 0;
              return Math.abs(h);
            }
            function color(node, dimmed) {
              if (pattern !== null && pattern.test(node.name)) return '#d63bd6';
              let hue = 45;
              if (node.name.startsWith('org.elasticsearch.')) hue = 18;
              else if (node.name.startsWith('org.apache.lucene.')) hue = 95;
              else if (node.name.startsWith('java.') || node.name.startsWith('jdk.') || node.name.startsWith('sun.')) hue = 200;
              const light = 58 + hash(node.name) % 14;
              return 'hsl(' + hue + ', ' + (dimmed ? 15 : 65) + '%, ' + light + '%)';
            }
            function draw() {
              const width = canvas.parentElement.clientWidth - 32;
              const height = (depth + 1) * ROW;
              const ratio = window.devicePixelRatio || 1;
              canvas.width = width * ratio;
              canvas.height = height * ratio;
              canvas.style.width = width + 'px';
              canvas.style.height = height + 'px';
              context.setTransform(ratio, 0, 0, ratio, 0, 0);
              context.clearRect(0, 0, width, height);
              context.font = '11px ui-monospace, Menlo, monospace';
              context.textBaseline = 'middle';
              const charWidth = context.measureText('m').width;
              placed = [];
              function frame(node, x, w, dimmed) {
                placed.push({ node: node, x: x, w: w });
                context.fillStyle = color(node, dimmed);
                context.fillRect(x, node.level * ROW, Math.max(w - 1, 0.5), ROW - 1);
                const fits = Math.floor((w - 6) / charWidth);
                if (fits >= 3) {
                  let label = shortName(node.name);
                  if (label.length > fits) label = label.substring(0, fits - 1) + '\\u2026';
                  context.fillStyle = '#000';
                  context.fillText(label, x + 3, node.level * ROW + ROW / 2);
                }
              }
              function layout(node, x, w) {
                frame(node, x, w, false);
                let left = x;
                for (const child of node.children) {
                  const childWidth = w * child.value / node.value;
                  if (childWidth >= 0.3) layout(child, left, childWidth);
                  left += childWidth;
                }
              }
              for (let above = zoomed.parent; above !== null; above = above.parent) frame(above, 0, width, true);
              layout(zoomed, 0, width);
            }
            function at(event) {
              const bounds = canvas.getBoundingClientRect();
              const x = event.clientX - bounds.left;
              const level = Math.floor((event.clientY - bounds.top) / ROW);
              return placed.find(p => p.node.level === level && x >= p.x && x < p.x + p.w) || null;
            }
            function percent(part, whole) {
              return (100 * part / whole).toFixed(2) + '%';
            }
            canvas.addEventListener('mousemove', event => {
              const hit = at(event);
              canvas.style.cursor = hit ? 'pointer' : 'default';
              status.textContent = hit === null ? '' : hit.node.name + '\\n' + hit.node.value + ' samples, '
                + percent(hit.node.value, root.value) + ' of all' + (zoomed === root ? '' : ', ' + percent(hit.node.value, zoomed.value) + ' of zoomed');
            });
            canvas.addEventListener('click', event => {
              const hit = at(event);
              if (hit !== null) {
                zoomed = hit.node;
                draw();
              }
            });
            document.getElementById('search').addEventListener('input', event => {
              try {
                pattern = event.target.value === '' ? null : new RegExp(event.target.value);
              } catch (e) {
                pattern = null;
              }
              let matched = 0;
              (function count(node) {
                if (pattern !== null && pattern.test(node.name)) matched += node.value;
                else for (const child of node.children) count(child);
              })(root);
              document.getElementById('matched').textContent = pattern === null ? '' : 'matched ' + percent(matched, root.value) + ' of samples';
              draw();
            });
            window.addEventListener('resize', draw);
            draw();
            </script>
            </body>
            </html>
            """;
}
