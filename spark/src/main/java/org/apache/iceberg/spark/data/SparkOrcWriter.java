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

package org.apache.iceberg.spark.data;

import java.util.List;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;

/**
 * This class acts as an adaptor from an OrcFileAppender to a
 * FileAppender&lt;InternalRow&gt;.
 */
public class SparkOrcWriter implements OrcValueWriter<InternalRow> {

  private final Converter[] converters;

  public SparkOrcWriter(TypeDescription schema) {
    converters = buildConverters(schema);
  }

  @Override
  public void write(InternalRow value, VectorizedRowBatch output) {
    int row = output.size++;
    for (int c = 0; c < converters.length; ++c) {
      converters[c].addValue(row, c, value, output.cols[c]);
    }
  }

  /**
   * The interface for the conversion from Spark's SpecializedGetters to
   * ORC's ColumnVectors.
   */
  interface Converter {
    /**
     * Take a value from the Spark data value and add it to the ORC output.
     * @param rowId the row in the ColumnVector
     * @param column either the column number or element number
     * @param data either an InternalRow or ArrayData
     * @param output the ColumnVector to put the value into
     */
    void addValue(int rowId, int column, SpecializedGetters data,
                  ColumnVector output);
  }

  static class BooleanConverter implements Converter {
    @Override
    public void addValue(int rowId, int column, SpecializedGetters data,
                         ColumnVector output) {
      if (data.isNullAt(column)) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((LongColumnVector) output).vector[rowId] = data.getBoolean(column) ? 1 : 0;
      }
    }
  }

  static class ByteConverter implements Converter {
    @Override
    public void addValue(int rowId, int column, SpecializedGetters data,
                         ColumnVector output) {
      if (data.isNullAt(column)) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((LongColumnVector) output).vector[rowId] = data.getByte(column);
      }
    }
  }

  static class ShortConverter implements Converter {
    @Override
    public void addValue(int rowId, int column, SpecializedGetters data,
                         ColumnVector output) {
      if (data.isNullAt(column)) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((LongColumnVector) output).vector[rowId] = data.getShort(column);
      }
    }
  }

  static class IntConverter implements Converter {
    @Override
    public void addValue(int rowId, int column, SpecializedGetters data,
                         ColumnVector output) {
      if (data.isNullAt(column)) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((LongColumnVector) output).vector[rowId] = data.getInt(column);
      }
    }
  }

  static class LongConverter implements Converter {
    @Override
    public void addValue(int rowId, int column, SpecializedGetters data,
                         ColumnVector output) {
      if (data.isNullAt(column)) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((LongColumnVector) output).vector[rowId] = data.getLong(column);
      }
    }
  }

  static class FloatConverter implements Converter {
    @Override
    public void addValue(int rowId, int column, SpecializedGetters data,
                         ColumnVector output) {
      if (data.isNullAt(column)) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((DoubleColumnVector) output).vector[rowId] = data.getFloat(column);
      }
    }
  }

  static class DoubleConverter implements Converter {
    @Override
    public void addValue(int rowId, int column, SpecializedGetters data,
                         ColumnVector output) {
      if (data.isNullAt(column)) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((DoubleColumnVector) output).vector[rowId] = data.getDouble(column);
      }
    }
  }

  static class StringConverter implements Converter {
    @Override
    public void addValue(int rowId, int column, SpecializedGetters data,
                         ColumnVector output) {
      if (data.isNullAt(column)) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        byte[] value = data.getUTF8String(column).getBytes();
        ((BytesColumnVector) output).setRef(rowId, value, 0, value.length);
      }
    }
  }

  static class BytesConverter implements Converter {
    @Override
    public void addValue(int rowId, int column, SpecializedGetters data,
                         ColumnVector output) {
      if (data.isNullAt(column)) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        // getBinary always makes a copy, so we don't need to worry about it
        // being changed behind our back.
        byte[] value = data.getBinary(column);
        ((BytesColumnVector) output).setRef(rowId, value, 0, value.length);
      }
    }
  }

  static class TimestampTzConverter implements Converter {
    @Override
    public void addValue(int rowId, int column, SpecializedGetters data,
                         ColumnVector output) {
      if (data.isNullAt(column)) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        TimestampColumnVector cv = (TimestampColumnVector) output;
        long micros = data.getLong(column);
        cv.time[rowId] = micros / 1_000; // millis
        cv.nanos[rowId] = (int) (micros % 1_000_000) * 1_000; // nanos
      }
    }
  }

  static class Decimal18Converter implements Converter {
    private final int precision;
    private final int scale;

    Decimal18Converter(TypeDescription schema) {
      precision = schema.getPrecision();
      scale = schema.getScale();
    }

    @Override
    public void addValue(int rowId, int column, SpecializedGetters data,
                         ColumnVector output) {
      if (data.isNullAt(column)) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((DecimalColumnVector) output).vector[rowId].setFromLongAndScale(
            data.getDecimal(column, precision, scale).toUnscaledLong(), scale);
      }
    }
  }

  static class Decimal38Converter implements Converter {
    private final int precision;
    private final int scale;

    Decimal38Converter(TypeDescription schema) {
      precision = schema.getPrecision();
      scale = schema.getScale();
    }

    @Override
    public void addValue(int rowId, int column, SpecializedGetters data,
                         ColumnVector output) {
      if (data.isNullAt(column)) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((DecimalColumnVector) output).vector[rowId].set(
            HiveDecimal.create(data.getDecimal(column, precision, scale)
                .toJavaBigDecimal()));
      }
    }
  }

  static class StructConverter implements Converter {
    private final Converter[] children;

    StructConverter(TypeDescription schema) {
      children = new Converter[schema.getChildren().size()];
      for (int c = 0; c < children.length; ++c) {
        children[c] = buildConverter(schema.getChildren().get(c));
      }
    }

    @Override
    public void addValue(int rowId, int column, SpecializedGetters data,
                         ColumnVector output) {
      if (data.isNullAt(column)) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        InternalRow value = data.getStruct(column, children.length);
        StructColumnVector cv = (StructColumnVector) output;
        for (int c = 0; c < children.length; ++c) {
          children[c].addValue(rowId, c, value, cv.fields[c]);
        }
      }
    }
  }

  static class ListConverter implements Converter {
    private final Converter children;

    ListConverter(TypeDescription schema) {
      children = buildConverter(schema.getChildren().get(0));
    }

    @Override
    public void addValue(int rowId, int column, SpecializedGetters data,
                         ColumnVector output) {
      if (data.isNullAt(column)) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ArrayData value = data.getArray(column);
        ListColumnVector cv = (ListColumnVector) output;
        // record the length and start of the list elements
        cv.lengths[rowId] = value.numElements();
        cv.offsets[rowId] = cv.childCount;
        cv.childCount += cv.lengths[rowId];
        // make sure the child is big enough
        cv.child.ensureSize(cv.childCount, true);
        // Add each element
        for (int e = 0; e < cv.lengths[rowId]; ++e) {
          children.addValue((int) (e + cv.offsets[rowId]), e, value, cv.child);
        }
      }
    }
  }

  static class MapConverter implements Converter {
    private final Converter keyConverter;
    private final Converter valueConverter;

    MapConverter(TypeDescription schema) {
      keyConverter = buildConverter(schema.getChildren().get(0));
      valueConverter = buildConverter(schema.getChildren().get(1));
    }

    @Override
    public void addValue(int rowId, int column, SpecializedGetters data,
                         ColumnVector output) {
      if (data.isNullAt(column)) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        MapData map = data.getMap(column);
        ArrayData key = map.keyArray();
        ArrayData value = map.valueArray();
        MapColumnVector cv = (MapColumnVector) output;
        // record the length and start of the list elements
        cv.lengths[rowId] = value.numElements();
        cv.offsets[rowId] = cv.childCount;
        cv.childCount += cv.lengths[rowId];
        // make sure the child is big enough
        cv.keys.ensureSize(cv.childCount, true);
        cv.values.ensureSize(cv.childCount, true);
        // Add each element
        for (int e = 0; e < cv.lengths[rowId]; ++e) {
          int pos = (int) (e + cv.offsets[rowId]);
          keyConverter.addValue(pos, e, key, cv.keys);
          valueConverter.addValue(pos, e, value, cv.values);
        }
      }
    }
  }

  private static Converter buildConverter(TypeDescription schema) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        return new BooleanConverter();
      case BYTE:
        return new ByteConverter();
      case SHORT:
        return new ShortConverter();
      case DATE:
      case INT:
        return new IntConverter();
      case LONG:
        return new LongConverter();
      case FLOAT:
        return new FloatConverter();
      case DOUBLE:
        return new DoubleConverter();
      case BINARY:
        return new BytesConverter();
      case STRING:
      case CHAR:
      case VARCHAR:
        return new StringConverter();
      case DECIMAL:
        return schema.getPrecision() <= 18 ?
            new Decimal18Converter(schema) :
            new Decimal38Converter(schema);
      case TIMESTAMP_INSTANT:
        return new TimestampTzConverter();
      case STRUCT:
        return new StructConverter(schema);
      case LIST:
        return new ListConverter(schema);
      case MAP:
        return new MapConverter(schema);
    }
    throw new IllegalArgumentException("Unhandled type " + schema);
  }

  private static Converter[] buildConverters(TypeDescription schema) {
    if (schema.getCategory() != TypeDescription.Category.STRUCT) {
      throw new IllegalArgumentException("Top level must be a struct " + schema);
    }
    List<TypeDescription> children = schema.getChildren();
    Converter[] result = new Converter[children.size()];
    for (int c = 0; c < children.size(); ++c) {
      result[c] = buildConverter(children.get(c));
    }
    return result;
  }

}
