/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.spark.actions;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampType;
import scala.collection.Seq;

class SparkZOrder implements Serializable {
    private final int STRING_KEY_LENGTH = 8;

    private final byte[] TINY_EMPTY = new byte[Byte.BYTES];
    private final byte[] SHORT_EMPTY = new byte[Short.BYTES];
    private final byte[] INT_EMPTY = new byte[Integer.BYTES];
    private final byte[] LONG_EMPTY = new byte[Long.BYTES];
    private final byte[] FLOAT_EMPTY = new byte[Float.BYTES];
    private final byte[] DOUBLE_EMPTY = new byte[Double.BYTES];

    transient private ThreadLocal<ByteBuffer> outputBuffer;
    transient private ThreadLocal<byte[][]> inputHolder;
    transient private ThreadLocal<ByteBuffer>[] inputBuffers;
    transient private ThreadLocal<CharsetEncoder> encoder;

    private final int numCols;

    private int inputCol = 0;
    private int totalBytes = 0;

    SparkZOrder(int numCols) {
        this.numCols = numCols;
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        inputBuffers = new ThreadLocal[numCols];
        inputHolder = ThreadLocal.withInitial(() -> new byte[numCols][]);
        encoder = ThreadLocal.withInitial(() -> StandardCharsets.UTF_8.newEncoder());
    }


    private ByteBuffer outputBuffer(int size) {
        if (outputBuffer == null) {
            // May over allocate on concurrent calls
            outputBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(size));
        }
        return outputBuffer.get().position(0);
    }

    private ByteBuffer inputBuffer(int position, int size){
        if (inputBuffers[position] == null) {
            // May over allocate on concurrent calls
            inputBuffers[position] = ThreadLocal.withInitial(() -> ByteBuffer.allocate(size));
        }
        return inputBuffers[position].get();
    }

    long interleaveBits(Seq<byte[]> scalaBinary) {
        byte[][] columnsBinary = scala.collection.JavaConverters.seqAsJavaList(scalaBinary)
                .toArray(inputHolder.get());
        ZOrderByteUtils.interleaveBits(columnsBinary, 8, outputBuffer(8));
        return outputBuffer(8).getLong();
    }

    private UserDefinedFunction tinyToOrderedBytesUDF() {
        int position = inputCol;
        UserDefinedFunction udf = functions.udf((Byte value) -> {
            if (value == null) {
                return TINY_EMPTY;
            }
            return ZOrderByteUtils.tinyintToOrderedBytes(value, inputBuffer(position, Byte.BYTES)).array();
        }, DataTypes.BinaryType).withName("TINY_ORDERED_BYTES");

        this.inputCol++;
        this.totalBytes+= Byte.BYTES;

        return udf;
    }

    private UserDefinedFunction shortToOrderedBytesUDF() {
        int position = inputCol;
        UserDefinedFunction udf = functions.udf((Short value) -> {
                    if (value == null) {
                        return SHORT_EMPTY;
                    }
                    return ZOrderByteUtils.shortToOrderedBytes(value, inputBuffer(position, Short.BYTES)).array();
                }, DataTypes.BinaryType)
                .withName("SHORT_ORDERED_BYTES");

        this.inputCol++;
        this.totalBytes+= Short.BYTES;

        return udf;
    }

    private UserDefinedFunction intToOrderedBytesUDF() {
        int position = inputCol;
        UserDefinedFunction udf = functions.udf((Integer value) -> {
                    if (value == null) {
                        return INT_EMPTY;
                    }
                    return ZOrderByteUtils.intToOrderedBytes(value, inputBuffer(position, Integer.BYTES)).array();
                }, DataTypes.BinaryType)
                .withName("INT_ORDERED_BYTES");

        this.inputCol++;
        this.totalBytes += Integer.BYTES;

        return udf;
    }

    private UserDefinedFunction longToOrderedBytesUDF() {
        int position = inputCol;
        UserDefinedFunction udf = functions.udf((Long value) -> {
                    if (value == null) {
                        return LONG_EMPTY;
                    }
                    return ZOrderByteUtils.longToOrderedBytes(value, inputBuffer(position, Long.BYTES)).array();
                }, DataTypes.BinaryType)
                .withName("LONG_ORDERED_BYTES");

        this.inputCol++;
        this.totalBytes += Long.BYTES;

        return udf;
    }

    private UserDefinedFunction floatToOrderedBytesUDF() {
        int position = inputCol;
        UserDefinedFunction  udf = functions.udf((Float value) -> {
                    if (value == null) {
                        return FLOAT_EMPTY;
                    }
                    return ZOrderByteUtils.floatToOrderedBytes(value, inputBuffer(position, Float.BYTES)).array();
                }, DataTypes.BinaryType)
                .withName("FLOAT_ORDERED_BYTES");

        this.inputCol++;
        this.totalBytes += Float.BYTES;

        return udf;
    }

    private UserDefinedFunction doubleToOrderedBytesUDF() {
        int position = inputCol;
        UserDefinedFunction udf = functions.udf((Double value) -> {
                    if (value == null) {
                        return DOUBLE_EMPTY;
                    }
                    return ZOrderByteUtils.doubleToOrderedBytes(value, inputBuffer(position, Double.BYTES)).array();
                }, DataTypes.BinaryType)
                .withName("FLOAT_ORDERED_BYTES");

        this.inputCol++;
        this.totalBytes += Double.BYTES;

        return udf;
    }

    private UserDefinedFunction stringToOrderedBytesUDF() {
        int position = inputCol;
        UserDefinedFunction udf = functions.udf((String value) ->
                ZOrderByteUtils.stringToOrderedBytes(value, STRING_KEY_LENGTH, inputBuffer(position, STRING_KEY_LENGTH),
                        encoder.get()).array(), DataTypes.BinaryType).withName("STRING-LEXICAL-BYTES");

        this.inputCol++;
        this.totalBytes += STRING_KEY_LENGTH;

        return udf;
    }

    private final UserDefinedFunction INTERLEAVE_UDF =
            functions.udf((Seq<byte[]> arrayBinary) -> interleaveBits(arrayBinary), DataTypes.LongType)
                    .withName("INTERLEAVE_BYTES");

    Column interleaveBytes(Column arrayBinary) {
        return INTERLEAVE_UDF.apply(arrayBinary);
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    Column sortedLexicographically(Column column, DataType type) {
        if (type instanceof ByteType) {
            return tinyToOrderedBytesUDF().apply(column);
        } else if (type instanceof ShortType) {
            return shortToOrderedBytesUDF().apply(column);
        } else if (type instanceof IntegerType) {
            return intToOrderedBytesUDF().apply(column);
        } else if (type instanceof LongType) {
            return longToOrderedBytesUDF().apply(column);
        } else if (type instanceof FloatType) {
            return floatToOrderedBytesUDF().apply(column);
        } else if (type instanceof DoubleType) {
            return doubleToOrderedBytesUDF().apply(column);
        } else if (type instanceof StringType) {
            return stringToOrderedBytesUDF().apply(column);
        } else if (type instanceof BinaryType) {
            return stringToOrderedBytesUDF().apply(column);
        } else if (type instanceof BooleanType) {
            return column.cast(DataTypes.BinaryType);
        } else if (type instanceof TimestampType) {
            return longToOrderedBytesUDF().apply(column.cast(DataTypes.LongType));
        } else if (type instanceof DateType) {
            return longToOrderedBytesUDF().apply(column.cast(DataTypes.LongType));
        } else {
            throw new IllegalArgumentException(
                    String.format("Cannot use column %s of type %s in ZOrdering, the type is unsupported",
                            column, type));
        }
    }
}