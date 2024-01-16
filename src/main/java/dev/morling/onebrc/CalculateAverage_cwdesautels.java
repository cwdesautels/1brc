/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.morling.onebrc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;

public class CalculateAverage_cwdesautels {
    private static final String FILE = "./measurements.txt";
    private static final int CORE_COUNT = Runtime.getRuntime().availableProcessors();
    private static final byte SEPARATOR = ';';
    private static final byte MINUS = '-';
    private static final byte ZERO = '0';
    private static final byte DOT = '.';
    private static final byte EOL = '\n';
    private static final int NAME_SIZE = 100;
    private static final int TEMP_SIZE = 4;

    public static void main(String[] args) throws InterruptedException, IOException {
        final long start = System.currentTimeMillis();
        final ChannelReader[] readers = new ChannelReader[CORE_COUNT];
        final Thread[] threads = new Thread[CORE_COUNT];
        final TreeMap<String, Stat> merged = new TreeMap<>();

        try (final var channel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            final long len = channel.size(), lenPerCore = len / CORE_COUNT;

            for (int i = CORE_COUNT - 1; i >= 0; i--) {
                readers[i] = new ChannelReader(channel, i * lenPerCore, lenPerCore, len);
                threads[i] = Thread.ofPlatform().start(readers[i]);
            }

            for (int j = CORE_COUNT - 1; j >= 0; j--) {
                threads[j].join();
                readers[j].merge(merged);
            }
        }

        System.out.println(merged);
        System.out.println("elapsed: " + (System.currentTimeMillis() - start) + "(ms)");
    }

    private static class ChannelReader implements Runnable {
        private final FileChannel channel;
        private final TreeMap<String, Stat> stats;
        private final long start;
        private final long size;
        private final long max;

        public ChannelReader(FileChannel channel, long start, long size, long len) {
            this.channel = channel;
            this.stats = new TreeMap<>();
            this.start = start;
            this.size = start + size > len
                    ? len - start
                    : size;
            this.max = start + this.size + NAME_SIZE + TEMP_SIZE > len
                    ? len - start
                    : this.size + NAME_SIZE + TEMP_SIZE;
        }

        @Override
        public void run() {
            try {
                final ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, start, max);
                int i = 0;
                int len = 0;

                // Skip first partial record
                if (start != 0) {
                    while (buffer.hasRemaining()) {
                        if (buffer.get() == EOL) {
                            break;
                        }
                    }
                }

                // Process records
                while (buffer.hasRemaining()) {
                    final byte data = buffer.get();

                    if (data == EOL) {
                        // Reset station name position
                        i = buffer.position();
                        len = 0;

                        // Terminate once we are past the size
                        if (i > size) {
                            break;
                        }
                    }
                    else if (data == SEPARATOR) {
                        // Take station up to the separator into shared byte array
                        final String name = StandardCharsets.UTF_8.decode(buffer.slice(i, len)).toString();

                        // Take temperature after the separator
                        final int t = getTemp(buffer);

                        // Upsert measurement
                        stats.compute(name, (__, stat) -> stat == null
                                ? new Stat(t)
                                : stat.add(t));
                    }
                    else {
                        len++;
                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private int getTemp(ByteBuffer buffer) {
            final byte b0 = buffer.get();
            final byte b1 = buffer.get();
            final byte b2 = buffer.get();

            if (b0 == MINUS) {
                final byte b3 = buffer.get();

                if (b2 != DOT) { // 6 bytes: -dd.dn
                    return -(((b1 - ZERO) * 10 + (b2 - ZERO)) * 10 + (b3 - ZERO));
                }
                else { // 5 bytes: -d.dn
                    return -((b1 - ZERO) * 10 + (b3 - ZERO));
                }
            }
            else {
                if (b1 != DOT) { // 5 bytes: dd.dn
                    final byte b3 = buffer.get();

                    return ((b0 - ZERO) * 10 + (b1 - ZERO)) * 10 + (b3 - ZERO);
                }
                else { // 4 bytes: d.dn
                    return (b0 - ZERO) * 10 + (b2 - ZERO);
                }
            }
        }

        private void merge(TreeMap<String, Stat> other) {
            stats.forEach((key, src) -> other
                    .compute(key, (__, dst) -> dst == null
                            ? src
                            : dst.merge(src)));
        }
    }

    private static class Stat {
        private int min;
        private int max;
        private int sum;
        private int count;

        private Stat(int v) {
            sum = min = max = v;
            count = 1;
        }

        private Stat add(int val) {
            min = Math.min(val, min);
            max = Math.max(val, max);
            sum += val;
            count++;

            return this;
        }

        private Stat merge(Stat other) {
            min = Math.min(other.min, min);
            max = Math.max(other.max, max);
            sum += other.sum;
            count += other.count;

            return this;
        }

        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min * 0.1, sum * 0.1 / count, max * 0.1);
        }
    }
}