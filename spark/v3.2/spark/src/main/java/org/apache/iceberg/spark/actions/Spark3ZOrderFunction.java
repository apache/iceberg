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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Function;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;

public class Spark3ZOrderFunction implements ScalarFunction<byte[]> {

    private final StructType inputType;
    private final DataType[] dataTypes;
    private final ThreadLocalBufferedZOrderFunction[] byteTransforms;
    transient private ThreadLocal<ByteBuffer>[] buffers;

    private final BufferedIntegerTransform transformA = new BufferedIntegerTransform();
    private final BufferedIntegerTransform transformB = new BufferedIntegerTransform();

    Spark3ZOrderFunction(StructType inputType) {
        this.inputType = inputType;
        this.dataTypes = Arrays.stream(inputType.fields()).map(StructField::dataType).toArray(DataType[]::new);
        this.byteTransforms = new ThreadLocalBufferedZOrderFunction[inputType.size()];
        for (int i = 0; i < dataTypes.length; i++) {
            byteTransforms[i] = transformForType(dataTypes[i]);
        }
    }

    public byte[] invoke(int a) {
        return transformA.apply(a);
    }

    public byte[] invoke(int a, int b) {
        byte[][] bytes = {transformA.apply(a), transformB.apply(b)};
        return ZOrderByteUtils.interleaveBits(bytes, 8);
    }

    private ThreadLocalBufferedZOrderFunction transformForType(DataType type) {
        //TODO
        return new BufferedIntegerTransform();
    }

    @Override
    public DataType[] inputTypes() {
        return dataTypes;
    }

    @Override
    public DataType resultType() {
        return DataTypes.BinaryType;
    }

    @Override
    public String name() {
        return String.format("Iceberg ZOrder for %s", inputType);
    }

    private byte[][] rowToBytes(InternalRow input) {
        //TODO for fallback case
        byte[][] byteRepresentation = new byte[input.numFields()][];
        return null;
    }

    @Override
    public byte[] produceResult(InternalRow input) {
        //TODO for fallback case
        throw new UnsupportedOperationException("Sorry");
    }

    public static UnboundFunction unbound() {
        return new UnboundFunction() {
            @Override
            public String name() {
                return "Iceberg ZOrder Function";
            }

            @Override
            public BoundFunction bind(StructType inputType) {
                return new Spark3ZOrderFunction(inputType);
            }

            @Override
            public String description() {
                return "Takes any number of columns are returns a Z-Value as an array of unordered bytes";
            }
        };
    }

    abstract class ThreadLocalBufferedZOrderFunction implements Serializable {
        transient ThreadLocal<ByteBuffer> buffer;
        final int size;
        ThreadLocalBufferedZOrderFunction(int size) {
            this.size = size;
        }

        protected ByteBuffer buffer() {
            if (buffer == null) {
                buffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(size));
            }
            return buffer.get();
        }
    }

    class BufferedTinyIntTransform extends ThreadLocalBufferedZOrderFunction {
        private byte[] empty = new byte[Byte.BYTES];
        BufferedTinyIntTransform() {
            super(Byte.BYTES);
        }

        public byte[] apply(byte value) {
            return ZOrderByteUtils.tinyintToOrderedBytes(value, buffer()).array();
        }

        public byte[] apply(Byte value) {
            if (value == null) {
               return empty;
            }
            return ZOrderByteUtils.tinyintToOrderedBytes(value, buffer()).array();
        }
    }

    class BufferedShortTransform extends ThreadLocalBufferedZOrderFunction {
        private byte[] empty = new byte[Short.BYTES];
        BufferedShortTransform() {
            super(Short.BYTES);
        }

        public byte[] apply(short value) {
            return ZOrderByteUtils.shortToOrderedBytes(value, buffer()).array();
        }

        public byte[] apply(Short value) {
            if (value == null) {
                return empty;
            }
            return ZOrderByteUtils.shortToOrderedBytes(value, buffer()).array();
        }
    }

    class BufferedIntegerTransform extends ThreadLocalBufferedZOrderFunction {
        private byte[] empty = new byte[Integer.BYTES];
        BufferedIntegerTransform() {
            super(Integer.BYTES);
        }

        public byte[] apply(int value) {
            return ZOrderByteUtils.intToOrderedBytes(value, buffer()).array();
        }

        public byte[] apply(Integer value) {
            if (value == null) {
                return empty;
            }
            return ZOrderByteUtils.intToOrderedBytes(value, buffer()).array();
        }
    }

    class BufferedLongTransform extends ThreadLocalBufferedZOrderFunction {
        private byte[] empty = new byte[Long.BYTES];
        BufferedLongTransform() {
            super(Long.BYTES);
        }

        public byte[] apply(long value) {
            return ZOrderByteUtils.longToOrderedBytes(value, buffer()).array();
        }

        public byte[] apply(Long value) {
            if (value == null) {
                return empty;
            }
            return ZOrderByteUtils.longToOrderedBytes(value, buffer()).array();
        }
    }

    class BufferedFloatTransform extends ThreadLocalBufferedZOrderFunction {
        private byte[] empty = new byte[Float.BYTES];
        BufferedFloatTransform() {
            super(Float.BYTES);
        }

        public byte[] apply(float value) {
            return ZOrderByteUtils.floatToOrderedBytes(value, buffer()).array();
        }

        public byte[] apply(Float value) {
            if (value == null) {
                return empty;
            }
            return ZOrderByteUtils.floatToOrderedBytes(value, buffer()).array();
        }
    }

    class BufferedDoubleTransform extends ThreadLocalBufferedZOrderFunction {
        private byte[] empty = new byte[Double.BYTES];
        BufferedDoubleTransform() {
            super(Double.BYTES);
        }

        public byte[] apply(double value) {
            return ZOrderByteUtils.doubleToOrderedBytes(value, buffer()).array();
        }

        public byte[] apply(Double value) {
            if (value == null) {
                return empty;
            }
            return ZOrderByteUtils.doubleToOrderedBytes(value, buffer()).array();
        }
    }

    class BufferedStringTransform extends ThreadLocalBufferedZOrderFunction {
        transient ThreadLocal<CharsetEncoder> encoder;

        private CharsetEncoder encoder() {
            if (encoder == null) {
                encoder = ThreadLocal.withInitial(() -> StandardCharsets.UTF_8.newEncoder());
            }
            return encoder.get();
        }

        BufferedStringTransform() {
            super(128);
        }

        public byte[] apply(String value) {
            return ZOrderByteUtils.stringToOrderedBytes(value, 128, buffer(), encoder()).array();
        }
    }

}
