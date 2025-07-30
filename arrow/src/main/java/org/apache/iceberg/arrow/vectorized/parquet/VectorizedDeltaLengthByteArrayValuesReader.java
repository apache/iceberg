/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.arrow.vectorized.parquet;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.iceberg.arrow.ArrowAllocation;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

public class VectorizedDeltaLengthByteArrayValuesReader implements VectorizedValuesReader, AutoCloseable {

    private final VectorizedDeltaEncodedValuesReader lengthReader;
    private final CloseableGroup closeables;

    private ByteBufferInputStream in;
    private IntVector lengthsVector;
    private ByteBuffer byteBuffer;

    VectorizedDeltaLengthByteArrayValuesReader() {
        lengthReader = new VectorizedDeltaEncodedValuesReader();
        closeables = new CloseableGroup();
    }

    @Override
    public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
        lengthsVector = new IntVector("length-" + UUID.randomUUID(), ArrowAllocation.rootAllocator());
        closeables.addCloseable(lengthsVector);
        lengthReader.initFromPage(valueCount, in);
        lengthReader.readIntegers(lengthReader.getTotalValueCount(), lengthsVector, 0);
        this.in = in.remainingStream();
    }

    @Override
    public Binary readBinary(int len) {
        readValues(1, null, 0, x -> len, (f, i, v) -> byteBuffer = v);
        return Binary.fromReusedByteBuffer(byteBuffer);
    }

    @Override
    public void readBinary(int total, FieldVector vec, int rowId) {
        readValues(total, vec, rowId, x -> lengthsVector.get(x), (f, i, v) -> f.getDataBuffer().setBytes(i, v));
    }

    private void readValues(
            int total,
            FieldVector vec,
            int rowId,
            IntUnaryOperator getLength,
            BinaryOutputWriter outputWriter) {
        ByteBuffer buffer;
        int length;
        for (int i = 0; i < total; i++) {
            length = getLength.applyAsInt(rowId + i);
            try {
                buffer = in.slice(length);
            } catch (EOFException e) {
                throw new ParquetDecodingException("Failed to read " + length + " bytes");
            }
            outputWriter.write(vec, rowId + i, buffer);
        }
    }

    @Override
    public boolean readBoolean() {
        throw new UnsupportedOperationException("readBoolean is not supported");
    }

    @Override
    public byte readByte() {
        throw new UnsupportedOperationException("readByte is not supported");
    }

    @Override
    public short readShort() {
        throw new UnsupportedOperationException("readShort is not supported");
    }

    @Override
    public int readInteger() {
        throw new UnsupportedOperationException("readInteger is not supported");
    }

    @Override
    public long readLong() {
        throw new UnsupportedOperationException("readLong is not supported");
    }

    @Override
    public float readFloat() {
        throw new UnsupportedOperationException("readFloat is not supported");
    }

    @Override
    public double readDouble() {
        throw new UnsupportedOperationException("readDouble is not supported");
    }

    @Override
    public void readIntegers(int total, FieldVector vec, int rowId) {
        throw new UnsupportedOperationException("readIntegers is not supported");
    }

    @Override
    public void readLongs(int total, FieldVector vec, int rowId) {
        throw new UnsupportedOperationException("readLongs is not supported");
    }

    @Override
    public void readFloats(int total, FieldVector vec, int rowId) {
        throw new UnsupportedOperationException("readFloats is not supported");
    }

    @Override
    public void readDoubles(int total, FieldVector vec, int rowId) {
        throw new UnsupportedOperationException("readDoubles is not supported");
    }

    @Override
    public void close() throws Exception {
        closeables.close();
    }

    /** A functional interface to write binary values into a FieldVector */
    @FunctionalInterface
    interface BinaryOutputWriter {

        /**
         * A functional interface that can be used to write a binary value to a specified row in a
         * FieldVector
         *
         * @param vec a FieldVector to write the value into
         * @param index The offset to write to
         * @param val value to write
         */
        void write(FieldVector vec, long index, ByteBuffer val);
    }
}
