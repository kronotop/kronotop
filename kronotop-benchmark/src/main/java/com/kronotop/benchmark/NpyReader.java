/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.benchmark;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads NumPy .npy v1.0 files containing 2D float32 arrays via memory-mapped I/O.
 */
public class NpyReader implements Closeable {
    private static final byte[] MAGIC = {(byte) 0x93, 'N', 'U', 'M', 'P', 'Y'};
    private static final Pattern SHAPE_PATTERN = Pattern.compile("'shape':\\s*\\((\\d+),\\s*(\\d+)\\)");

    private final FileChannel channel;
    private final FloatBuffer data;
    private final int rows;
    private final int cols;

    public NpyReader(Path path) throws IOException {
        this.channel = FileChannel.open(path, StandardOpenOption.READ);

        // Read header: 6 magic + 1 major + 1 minor + 2 header_len
        ByteBuffer headerPrefix = ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN);
        channel.read(headerPrefix);
        headerPrefix.flip();

        // Validate magic
        for (byte b : MAGIC) {
            if (headerPrefix.get() != b) {
                throw new IOException("Not a valid .npy file");
            }
        }

        int major = headerPrefix.get() & 0xFF;
        headerPrefix.get(); // minor version, unused
        if (major != 1) {
            throw new IOException("Only .npy v1.0 is supported, got v" + major);
        }

        int headerLen = headerPrefix.getShort() & 0xFFFF;

        // Read header dict
        ByteBuffer headerBuf = ByteBuffer.allocate(headerLen);
        channel.read(headerBuf);
        headerBuf.flip();
        String header = new String(headerBuf.array(), 0, headerLen);

        // Validate dtype is little-endian float32
        if (!header.contains("'<f4'")) {
            throw new IOException("Only little-endian float32 (<f4) is supported. Header: " + header);
        }

        // Parse shape
        Matcher m = SHAPE_PATTERN.matcher(header);
        if (!m.find()) {
            throw new IOException("Could not parse shape from header: " + header);
        }
        this.rows = Integer.parseInt(m.group(1));
        this.cols = Integer.parseInt(m.group(2));

        // Memory-map the data section
        long dataOffset = 10L + headerLen;
        long dataSize = (long) rows * cols * Float.BYTES;
        ByteBuffer mapped = channel.map(FileChannel.MapMode.READ_ONLY, dataOffset, dataSize)
                .order(ByteOrder.LITTLE_ENDIAN);
        this.data = mapped.asFloatBuffer();
    }

    public int rows() {
        return rows;
    }

    public int cols() {
        return cols;
    }

    public float[] readRow(int index) {
        if (index < 0 || index >= rows) {
            throw new IndexOutOfBoundsException("Row " + index + " out of range [0, " + rows + ")");
        }
        float[] row = new float[cols];
        int offset = index * cols;
        for (int i = 0; i < cols; i++) {
            row[i] = data.get(offset + i);
        }
        return row;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
