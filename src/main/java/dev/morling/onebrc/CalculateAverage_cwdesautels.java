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
    private static final int CORE_COUNT = 1;//Runtime.getRuntime().availableProcessors();
    private static final byte SEPARATOR = ':';
    private static final byte MINUS = '-';
    private static final byte EOL = '\n';
    private static final byte DOT = '.';
    private static final long CHUNK_SIZE = 4096;

    public static void main(String[] args) throws InterruptedException, IOException {
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
    }

    private static class ChannelReader implements Runnable {
        private final FileChannel channel;
        private final TreeMap<String, Stat> stats;
        private final long start;
        private final long size;
        private final long sizeWithOverflow;

        public ChannelReader(FileChannel channel, long start, long size, long len) {
            this.channel = channel;
            this.stats = new TreeMap<>();
            this.start = start;
            this.size = start + size > len
                    ? len - start
                    : size;
            this.sizeWithOverflow = start + size + 16 > len
                    ? len - start
                    : size + 16;

            System.out.printf("size: %d, len: %d, of: %d, start: %d, end: %d%n", size, len, sizeWithOverflow, start, start + size);
        }

        private void skipLine(ByteBuffer buffer) {
            while (buffer.hasRemaining()) {
                if (buffer.get() == EOL) {
                    break;
                }
            }
        }

        @Override
        public void run() {
            // parse file
            // if start is incomplete, ignore
            // if end is incomplete continue

            try {
                final ByteBuffer nameBuf = ByteBuffer.allocate(32);
                final ByteBuffer tempBuf = ByteBuffer.allocate(32);
                final ByteBuffer inputBuf = channel.map(FileChannel.MapMode.READ_ONLY, start, sizeWithOverflow);

                boolean nameEnded = false;

                // Skip first partial record
                if (start != 0) {
                    skipLine(inputBuf);
                }

                // Process records
                // TODO: track only offsets and copy once into target buffers
                while (inputBuf.hasRemaining()) {
                    final byte data = inputBuf.get();

                    switch (data) {
                        // case MINUS:
                        // // one more
                        // continue;
                        case DOT:
                            // get one more, short cut to EOL
                            if (inputBuf.hasRemaining()) {
                                tempBuf.put(inputBuf.get());
                            }

                            skipLine(inputBuf);
                        case EOL:
                            // confirm end of temp, start of name

                            final String name = StandardCharsets.UTF_8.decode(nameBuf).toString();
                            final int temp = tempBuf.getInt();

                            stats.compute(name, (__, stat) -> stat == null
                                    ? new Stat(temp)
                                    : stat.add(temp));

                            nameBuf.clear();
                            tempBuf.clear();
                            nameEnded = false;

                            if (inputBuf.position() >= start + size) {
                                break;
                            }

                            continue;
                        case SEPARATOR:
                            nameEnded = true;
                            continue;
                        default:
                            if (nameEnded) {
                                tempBuf.put(data);
                            } else {
                                nameBuf.put(data);
                            }
                    }
                }
                // }
            } catch (IOException e) {
                throw new RuntimeException(e);
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
        private long sum;
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