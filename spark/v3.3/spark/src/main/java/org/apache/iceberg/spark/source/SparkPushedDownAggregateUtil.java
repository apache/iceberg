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
package org.apache.iceberg.spark.source;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnboundAggregate;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

/** Helper methods for working with Spark aggregate push down. */
public class SparkPushedDownAggregateUtil {

  private SparkPushedDownAggregateUtil() {}

  // Build schema for pushed down aggregates. This schema will be used as the scan schema.
  public static StructType buildSchemaForPushedDownAggregate(
      List<Expression> aggregates, boolean caseSensitive, Schema schema) {
    StructType finalSchema = new StructType();
    for (int index = 0; index < aggregates.size(); index++) {
      if ((aggregates.get(index)).op().name().equals("COUNTSTAR")) {
        StructField field =
            new StructField("COUNT(*)", DataTypes.LongType, false, Metadata.empty());
        finalSchema = finalSchema.add(field);
      } else {
        String colName = ((UnboundAggregate) aggregates.get(index)).ref().name();
        DataType dataType = getDataTypeForAggregateColumn(colName, caseSensitive, schema);
        if (dataType instanceof StructType
            || dataType instanceof ArrayType
            || dataType instanceof MapType) {
          // not building pushed down aggregate schema for complex types to disable aggregate push
          // down because the statistic info for complex are not available.
          return finalSchema;
        }
        if ((aggregates.get(index)).op().name().equals("COUNT")) {
          StructField field =
              new StructField(
                  "COUNT(" + colName + ")", DataTypes.LongType, false, Metadata.empty());
          finalSchema = finalSchema.add(field);
        } else if ((aggregates.get(index)).op().name().equals("MAX")) {
          StructField field =
              new StructField("MAX(" + colName + ")", dataType, false, Metadata.empty());
          finalSchema = finalSchema.add(field);
        } else if ((aggregates.get(index)).op().name().equals("MIN")) {
          StructField field =
              new StructField("MIN(" + colName + ")", dataType, false, Metadata.empty());
          finalSchema = finalSchema.add(field);
        }
      }
    }
    return finalSchema;
  }

  public static DataType getDataTypeForAggregateColumn(
      String colName, boolean caseSensitive, Schema schema) {
    Type type = null;
    for (int i = 0; i < schema.columns().size(); i++) {
      if ((caseSensitive && schema.columns().get(i).name().equals(colName))
          || (!caseSensitive && schema.columns().get(i).name().equalsIgnoreCase(colName))) {
        type = schema.columns().get(i).type();
      }
    }
    return SparkSchemaUtil.convert(type);
  }

  /**
   * get "lower_bounds", "upper_bounds", "record_count", "null_value_counts" from metadata table and
   * use these to calculate Max/Min/Count, and then use the values of Max/Min/Count to construct an
   * InternalRow to return to Spark.
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static InternalRow[] constructInternalRowForPushedDownAggregate(
      SparkSession spark, Table table, StructType pushedAggregateSchema, boolean caseSensitive) {
    // get the statistics info from DATA_FILES, calculate the aggregate values (min/max/count)
    // and use these value to build an internalRow.
    Dataset<Row> metadataRows =
        SparkTableUtil.loadMetadataTable(spark, table, MetadataTableType.DATA_FILES);

    Dataset dataset =
        metadataRows.selectExpr(
            "lower_bounds", "upper_bounds", "record_count", "null_value_counts");
    Row[] statisticRows = (Row[]) dataset.collect();

    StructField[] fields = pushedAggregateSchema.fields();
    List<Object> valuesInSparkInternalRow = Lists.newArrayList();

    for (int i = 0; i < fields.length; i++) {
      if (fields[i].name().contains("MIN")) {
        int index = indexInTableSchema(fields[i], table, caseSensitive);
        Object min = getMinOrMax(statisticRows, index + 1, fields[i], true);
        if (min == null) {
          return null; // if min is not available, don't push down aggregate
        }
        valuesInSparkInternalRow.add(min);
      } else if (fields[i].name().contains("MAX")) {
        int index = indexInTableSchema(fields[i], table, caseSensitive);
        Object max = getMinOrMax(statisticRows, index + 1, fields[i], false);
        if (max == null) {
          return null; // if max is not available, don't push down aggregate
        }
        valuesInSparkInternalRow.add(max);
      } else if (fields[i].name().contains("COUNT(*)")) {
        long count = 0;
        for (int j = 0; j < statisticRows.length; j++) {
          count += statisticRows[j].getLong(2); // record_count is the 3rd column in staticticRows
        }
        valuesInSparkInternalRow.add(count);
      } else if (fields[i].name().contains("COUNT")) {
        long count = 0;
        for (int j = 0; j < statisticRows.length; j++) {
          count += statisticRows[j].getLong(2); // record_count is the 3rd column in staticticRows
        }
        int index = indexInTableSchema(fields[i], table, caseSensitive);
        long numOfNulls = getNullValueCount(statisticRows, index + 1);
        valuesInSparkInternalRow.add(count - numOfNulls);
      }
    }
    InternalRow[] rows = new InternalRow[1];
    rows[0] = InternalRow.fromSeq(JavaConverters.asScalaBuffer(valuesInSparkInternalRow).toSeq());
    return rows;
  }

  private static int indexInTableSchema(StructField field, Table table, boolean caseSensitive) {
    StructField[] sparkTableFields = SparkSchemaUtil.convert(table.schema()).fields();
    for (int i = 0; i < sparkTableFields.length; i++) {
      if (caseSensitive) {
        if (field.name().contains(sparkTableFields[i].name())) {
          return i;
        }
      } else {
        if (field
            .name()
            .toLowerCase(Locale.ROOT)
            .contains(sparkTableFields[i].name().toLowerCase(Locale.ROOT))) {
          return i;
        }
      }
    }
    return -1; // this should never happen
  }

  private static long getNullValueCount(Row[] nullValueCount, int index) {
    long numOfNlls = 0L;
    for (int i = 0; i < nullValueCount.length; i++) {
      // null_value_count is the 4th column in staticticRows
      Map<Integer, Long> map = nullValueCount[i].getJavaMap(3);
      Long value = map.get(index);
      numOfNlls += value;
    }
    return numOfNlls;
  }

  /** loop through "lower_bounds" or "upper_bounds" to get min or max */
  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "MethodLength"})
  private static Object getMinOrMax(Row[] rows, int index, StructField field, boolean isMin) {
    Object result = null;
    boolean isString = false;
    boolean isBinary = false;
    boolean isDecimal = false;
    int columIndex = 0; // lower_bound is the 1st column in staticticRows
    if (!isMin) {
      columIndex = 1; // upper_bound is the 2nd column in staticticRows
    }

    for (int i = 0; i < rows.length; i++) {
      Map<Integer, byte[]> map = rows[i].getJavaMap(columIndex);
      byte[] valueInBytes = map.get(index);
      if (valueInBytes != null) {
        Type type = SparkSchemaUtil.convert(field.dataType());
        switch (type.typeId()) {
          case BOOLEAN:
            boolean booleanValue =
                Conversions.fromByteBuffer(
                    Types.BooleanType.get(), ByteBuffer.wrap(map.get(index)));
            if (isMin) {
              if (result == null
                  || Literal.of(booleanValue).comparator().compare(booleanValue, (boolean) result)
                      < 0) {
                result = booleanValue;
              }
            } else {
              if (result == null
                  || Literal.of(booleanValue).comparator().compare(booleanValue, (boolean) result)
                      > 0) {
                result = booleanValue;
              }
            }
            break;
          case INTEGER:
          case DATE:
            int intValue =
                Conversions.fromByteBuffer(
                    Types.IntegerType.get(), ByteBuffer.wrap(map.get(index)));
            if (isMin) {
              if (result == null
                  || (Literal.of(intValue)).comparator().compare(intValue, (int) result) < 0) {
                result = intValue;
              }
            } else {
              if (result == null
                  || (Literal.of(intValue)).comparator().compare(intValue, (int) result) > 0) {
                result = intValue;
              }
            }
            break;
          case LONG:
          case TIME:
          case TIMESTAMP:
            long longValue =
                Conversions.fromByteBuffer(Types.LongType.get(), ByteBuffer.wrap(map.get(index)));
            if (isMin) {
              if (result == null
                  || (Literal.of(longValue)).comparator().compare(longValue, (Long) result) < 0) {
                result = longValue;
              }
            } else {
              if (result == null
                  || (Literal.of(longValue)).comparator().compare(longValue, (Long) result) > 0) {
                result = longValue;
              }
            }
            break;
          case FLOAT:
            float fValue =
                Conversions.fromByteBuffer(Types.FloatType.get(), ByteBuffer.wrap(map.get(index)));
            if (isMin) {
              if (result == null
                  || (Literal.of(fValue)).comparator().compare(fValue, (float) result) < 0) {
                result = fValue;
              }
            } else {
              if (result == null
                  || (Literal.of(fValue)).comparator().compare(fValue, (float) result) > 0) {
                result = fValue;
              }
            }
            break;
          case DOUBLE:
            double doubleValue =
                Conversions.fromByteBuffer(Types.DoubleType.get(), ByteBuffer.wrap(map.get(index)));
            if (isMin) {
              if (result == null
                  || (Literal.of(doubleValue)).comparator().compare(doubleValue, (double) result)
                      < 0) {
                result = doubleValue;
              }
            } else {
              if (result == null
                  || (Literal.of(doubleValue)).comparator().compare(doubleValue, (double) result)
                      > 0) {
                result = doubleValue;
              }
            }
            break;
          case STRING:
            String stringValue =
                Conversions.fromByteBuffer(Types.StringType.get(), ByteBuffer.wrap(map.get(index)))
                    .toString();
            if (isMin) {
              if (result == null
                  || (Literal.of(stringValue)).comparator().compare(stringValue, (String) result)
                      < 0) {
                result = stringValue;
              }
            } else {
              if (result == null
                  || (Literal.of(stringValue)).comparator().compare(stringValue, (String) result)
                      > 0) {
                result = stringValue;
              }
            }
            isString = true;
            break;
          case FIXED:
          case BINARY:
            ByteBuffer binaryValue =
                Conversions.fromByteBuffer(Types.BinaryType.get(), ByteBuffer.wrap(map.get(index)));
            if (isMin) {
              if (result == null
                  || (Literal.of(binaryValue))
                          .comparator()
                          .compare(binaryValue, (ByteBuffer) result)
                      < 0) {
                result = binaryValue;
              }
            } else {
              if (result == null
                  || (Literal.of(binaryValue))
                          .comparator()
                          .compare(binaryValue, (ByteBuffer) result)
                      > 0) {
                result = binaryValue;
              }
            }
            isBinary = true;
            break;
          case DECIMAL:
            int precision = ((DecimalType) field.dataType()).precision();
            int scale = ((DecimalType) field.dataType()).scale();
            BigDecimal decimal =
                Conversions.fromByteBuffer(
                    Types.DecimalType.of(precision, scale), ByteBuffer.wrap(map.get(index)));
            if (isMin) {
              if (result == null
                  || (Literal.of(decimal)).comparator().compare(decimal, (BigDecimal) result) < 0) {
                result = decimal;
              }
            } else {
              if (result == null
                  || (Literal.of(decimal)).comparator().compare(decimal, (BigDecimal) result) > 0) {
                result = decimal;
              }
            }
            isDecimal = true;
            break;
          default:
            return null;
        }
      }
    }
    if (isString) {
      return org.apache.spark.unsafe.types.UTF8String.fromString(result.toString());
    }
    if (isBinary) {
      byte[] arr = new byte[((ByteBuffer) result).remaining()];
      ((ByteBuffer) result).get(arr);
      return arr;
    }
    if (isDecimal) {
      return Decimal.apply(new scala.math.BigDecimal((BigDecimal) result));
    }
    return result;
  }
}
