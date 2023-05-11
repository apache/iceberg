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
package org.apache.iceberg.spark.data.vectorized;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.orc.OrcBatchReader;
import org.apache.iceberg.orc.OrcSchemaWithTypeVisitor;
import org.apache.iceberg.orc.OrcValueReader;
import org.apache.iceberg.orc.OrcValueReaders;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.SparkOrcValueReaders;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public class VectorizedSparkOrcReaders {

  private VectorizedSparkOrcReaders() {}

  public static OrcBatchReader<ColumnarBatch> buildReader(
      Schema expectedSchema, TypeDescription fileSchema, Map<Integer, ?> idToConstant) {
    Converter converter =
        OrcSchemaWithTypeVisitor.visit(expectedSchema, fileSchema, new ReadBuilder(idToConstant));

    return new OrcBatchReader<ColumnarBatch>() {
      private long batchOffsetInFile;

      @Override
      public ColumnarBatch read(VectorizedRowBatch batch) {
        BaseOrcColumnVector cv =
            (BaseOrcColumnVector)
                converter.convert(
                    new StructColumnVector(batch.size, batch.cols), batch.size, batchOffsetInFile);
        ColumnarBatch columnarBatch =
            new ColumnarBatch(
                IntStream.range(0, expectedSchema.columns().size())
                    .mapToObj(cv::getChild)
                    .toArray(ColumnVector[]::new));
        columnarBatch.setNumRows(batch.size);
        return columnarBatch;
      }

      @Override
      public void setBatchContext(long batchOffsetInFile) {
        this.batchOffsetInFile = batchOffsetInFile;
      }
    };
  }

  private interface Converter {
    ColumnVector convert(
        org.apache.orc.storage.ql.exec.vector.ColumnVector columnVector,
        int batchSize,
        long batchOffsetInFile);
  }

  private static class ReadBuilder extends OrcSchemaWithTypeVisitor<Converter> {
    private final Map<Integer, ?> idToConstant;

    private ReadBuilder(Map<Integer, ?> idToConstant) {
      this.idToConstant = idToConstant;
    }

    @Override
    public Converter record(
        Types.StructType iStruct,
        TypeDescription record,
        List<String> names,
        List<Converter> fields) {
      return new StructConverter(iStruct, fields, idToConstant);
    }

    @Override
    public Converter list(Types.ListType iList, TypeDescription array, Converter element) {
      return new ArrayConverter(iList, element);
    }

    @Override
    public Converter map(Types.MapType iMap, TypeDescription map, Converter key, Converter value) {
      return new MapConverter(iMap, key, value);
    }

    @Override
    public Converter primitive(Type.PrimitiveType iPrimitive, TypeDescription primitive) {
      final OrcValueReader<?> primitiveValueReader;
      switch (primitive.getCategory()) {
        case BOOLEAN:
          primitiveValueReader = OrcValueReaders.booleans();
          break;
        case BYTE:
          // Iceberg does not have a byte type. Use int
        case SHORT:
          // Iceberg does not have a short type. Use int
        case DATE:
        case INT:
          primitiveValueReader = OrcValueReaders.ints();
          break;
        case LONG:
          primitiveValueReader = OrcValueReaders.longs();
          break;
        case FLOAT:
          primitiveValueReader = OrcValueReaders.floats();
          break;
        case DOUBLE:
          primitiveValueReader = OrcValueReaders.doubles();
          break;
        case TIMESTAMP_INSTANT:
        case TIMESTAMP:
          primitiveValueReader = SparkOrcValueReaders.timestampTzs();
          break;
        case DECIMAL:
          primitiveValueReader =
              SparkOrcValueReaders.decimals(primitive.getPrecision(), primitive.getScale());
          break;
        case CHAR:
        case VARCHAR:
        case STRING:
          primitiveValueReader = SparkOrcValueReaders.utf8String();
          break;
        case BINARY:
          primitiveValueReader =
              Type.TypeID.UUID == iPrimitive.typeId()
                  ? SparkOrcValueReaders.uuids()
                  : OrcValueReaders.bytes();
          break;
        default:
          throw new IllegalArgumentException("Unhandled type " + primitive);
      }
      return (columnVector, batchSize, batchOffsetInFile) ->
          new PrimitiveOrcColumnVector(
              iPrimitive, batchSize, columnVector, primitiveValueReader, batchOffsetInFile);
    }
  }

  private abstract static class BaseOrcColumnVector extends ColumnVector {
    private final org.apache.orc.storage.ql.exec.vector.ColumnVector vector;
    private final int batchSize;
    private Integer numNulls;

    BaseOrcColumnVector(
        Type type, int batchSize, org.apache.orc.storage.ql.exec.vector.ColumnVector vector) {
      super(SparkSchemaUtil.convert(type));
      this.vector = vector;
      this.batchSize = batchSize;
    }

    @Override
    public void close() {}

    @Override
    public boolean hasNull() {
      return !vector.noNulls;
    }

    @Override
    public int numNulls() {
      if (numNulls == null) {
        numNulls = numNullsHelper();
      }
      return numNulls;
    }

    private int numNullsHelper() {
      if (vector.isRepeating) {
        if (vector.isNull[0]) {
          return batchSize;
        } else {
          return 0;
        }
      } else if (vector.noNulls) {
        return 0;
      } else {
        int count = 0;
        for (int i = 0; i < batchSize; i++) {
          if (vector.isNull[i]) {
            count++;
          }
        }
        return count;
      }
    }

    protected int getRowIndex(int rowId) {
      return vector.isRepeating ? 0 : rowId;
    }

    @Override
    public boolean isNullAt(int rowId) {
      return vector.isNull[getRowIndex(rowId)];
    }

    @Override
    public boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException();
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ColumnarArray getArray(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ColumnarMap getMap(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      throw new UnsupportedOperationException();
    }
  }

  private static class PrimitiveOrcColumnVector extends BaseOrcColumnVector {
    private final org.apache.orc.storage.ql.exec.vector.ColumnVector vector;
    private final OrcValueReader<?> primitiveValueReader;
    private final long batchOffsetInFile;

    PrimitiveOrcColumnVector(
        Type type,
        int batchSize,
        org.apache.orc.storage.ql.exec.vector.ColumnVector vector,
        OrcValueReader<?> primitiveValueReader,
        long batchOffsetInFile) {
      super(type, batchSize, vector);
      this.vector = vector;
      this.primitiveValueReader = primitiveValueReader;
      this.batchOffsetInFile = batchOffsetInFile;
    }

    @Override
    public boolean getBoolean(int rowId) {
      return (Boolean) primitiveValueReader.read(vector, rowId);
    }

    @Override
    public int getInt(int rowId) {
      return (Integer) primitiveValueReader.read(vector, rowId);
    }

    @Override
    public long getLong(int rowId) {
      return (Long) primitiveValueReader.read(vector, rowId);
    }

    @Override
    public float getFloat(int rowId) {
      return (Float) primitiveValueReader.read(vector, rowId);
    }

    @Override
    public double getDouble(int rowId) {
      return (Double) primitiveValueReader.read(vector, rowId);
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
      // TODO: Is it okay to assume that (precision,scale) parameters == (precision,scale) of the
      // decimal type
      // and return a Decimal with (precision,scale) of the decimal type?
      return (Decimal) primitiveValueReader.read(vector, rowId);
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
      return (UTF8String) primitiveValueReader.read(vector, rowId);
    }

    @Override
    public byte[] getBinary(int rowId) {
      return (byte[]) primitiveValueReader.read(vector, rowId);
    }
  }

  private static class ArrayConverter implements Converter {
    private final Types.ListType listType;
    private final Converter elementConverter;

    private ArrayConverter(Types.ListType listType, Converter elementConverter) {
      this.listType = listType;
      this.elementConverter = elementConverter;
    }

    @Override
    public ColumnVector convert(
        org.apache.orc.storage.ql.exec.vector.ColumnVector vector,
        int batchSize,
        long batchOffsetInFile) {
      ListColumnVector listVector = (ListColumnVector) vector;
      ColumnVector elementVector =
          elementConverter.convert(listVector.child, batchSize, batchOffsetInFile);

      return new BaseOrcColumnVector(listType, batchSize, vector) {
        @Override
        public ColumnarArray getArray(int rowId) {
          int index = getRowIndex(rowId);
          return new ColumnarArray(
              elementVector, (int) listVector.offsets[index], (int) listVector.lengths[index]);
        }
      };
    }
  }

  private static class MapConverter implements Converter {
    private final Types.MapType mapType;
    private final Converter keyConverter;
    private final Converter valueConverter;

    private MapConverter(Types.MapType mapType, Converter keyConverter, Converter valueConverter) {
      this.mapType = mapType;
      this.keyConverter = keyConverter;
      this.valueConverter = valueConverter;
    }

    @Override
    public ColumnVector convert(
        org.apache.orc.storage.ql.exec.vector.ColumnVector vector,
        int batchSize,
        long batchOffsetInFile) {
      MapColumnVector mapVector = (MapColumnVector) vector;
      ColumnVector keyVector = keyConverter.convert(mapVector.keys, batchSize, batchOffsetInFile);
      ColumnVector valueVector =
          valueConverter.convert(mapVector.values, batchSize, batchOffsetInFile);

      return new BaseOrcColumnVector(mapType, batchSize, vector) {
        @Override
        public ColumnarMap getMap(int rowId) {
          int index = getRowIndex(rowId);
          return new ColumnarMap(
              keyVector,
              valueVector,
              (int) mapVector.offsets[index],
              (int) mapVector.lengths[index]);
        }
      };
    }
  }

  private static class StructConverter implements Converter {
    private final Types.StructType structType;
    private final List<Converter> fieldConverters;
    private final Map<Integer, ?> idToConstant;

    private StructConverter(
        Types.StructType structType,
        List<Converter> fieldConverters,
        Map<Integer, ?> idToConstant) {
      this.structType = structType;
      this.fieldConverters = fieldConverters;
      this.idToConstant = idToConstant;
    }

    @Override
    public ColumnVector convert(
        org.apache.orc.storage.ql.exec.vector.ColumnVector vector,
        int batchSize,
        long batchOffsetInFile) {
      StructColumnVector structVector = (StructColumnVector) vector;
      List<Types.NestedField> fields = structType.fields();
      List<ColumnVector> fieldVectors = Lists.newArrayListWithExpectedSize(fields.size());
      for (int pos = 0, vectorIndex = 0; pos < fields.size(); pos += 1) {
        Types.NestedField field = fields.get(pos);
        if (idToConstant.containsKey(field.fieldId())) {
          fieldVectors.add(
              new ConstantColumnVector(field.type(), batchSize, idToConstant.get(field.fieldId())));
        } else if (field.equals(MetadataColumns.ROW_POSITION)) {
          fieldVectors.add(new RowPositionColumnVector(batchOffsetInFile));
        } else if (field.equals(MetadataColumns.IS_DELETED)) {
          fieldVectors.add(new ConstantColumnVector(field.type(), batchSize, false));
        } else {
          fieldVectors.add(
              fieldConverters
                  .get(vectorIndex)
                  .convert(structVector.fields[vectorIndex], batchSize, batchOffsetInFile));
          vectorIndex++;
        }
      }

      return new BaseOrcColumnVector(structType, batchSize, vector) {
        @Override
        public ColumnVector getChild(int ordinal) {
          return fieldVectors.get(ordinal);
        }
      };
    }
  }
}
