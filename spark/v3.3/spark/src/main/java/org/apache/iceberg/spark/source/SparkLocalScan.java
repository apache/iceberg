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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.LocalScan;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

class SparkLocalScan implements LocalScan {

  private final SparkSession spark;
  private final Table table;
  private final List<Expression> aggregateExpressions;
  private final boolean caseSensitive;
  private final StructType aggregateSchema;

  SparkLocalScan(
      SparkSession spark,
      Table table,
      SparkReadConf readConf,
      List<Expression> aggregates,
      StructType aggregateSchema) {
    this.spark = spark;
    this.table = table;
    this.caseSensitive = readConf.caseSensitive();
    this.aggregateExpressions = aggregates != null ? aggregates : Collections.emptyList();
    this.aggregateSchema = aggregateSchema;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  public InternalRow[] rows() {
    // get the statistics info from DATA_FILES, calculate the aggregate values (min/max/count)
    // and use these value to build an internalRow.
    Dataset<Row> metadataRows =
        SparkTableUtil.loadMetadataTable(spark, table, MetadataTableType.DATA_FILES);

    Row[] lowerBounds = null;
    Row[] upperBounds = null;
    Row[] recordCount = null;
    Row[] nullValueCount = null;

    StructField[] fields = aggregateSchema.fields();
    List<Object> valuesInSparkInternalRow = Lists.newArrayList();
    InternalRow[] rows = new InternalRow[1];

    for (int i = 0; i < fields.length; i++) {
      if (fields[i].name().contains("MIN")) {
        if (lowerBounds == null) {
          lowerBounds = (Row[]) metadataRows.selectExpr("lower_bounds").collect();
        }
        int index = indexInTableSchema(fields[i]);
        Object min = getMinOrMax(lowerBounds, index + 1, fields[i], true);
        valuesInSparkInternalRow.add(min);
      } else if (fields[i].name().contains("MAX")) {
        if (upperBounds == null) {
          upperBounds = (Row[]) metadataRows.selectExpr("upper_bounds").collect();
        }
        int index = indexInTableSchema(fields[i]);
        Object max = getMinOrMax(upperBounds, index + 1, fields[i], false);
        valuesInSparkInternalRow.add(max);
      } else if (fields[i].name().contains("COUNT(*)")) {
        if (recordCount == null) {
          recordCount = (Row[]) metadataRows.selectExpr("record_count").collect();
        }
        long count = 0;
        for (int j = 0; j < recordCount.length; j++) {
          count += recordCount[j].getLong(0);
        }
        valuesInSparkInternalRow.add(count);
      } else if (fields[i].name().contains("COUNT")) {
        if (recordCount == null) {
          recordCount = (Row[]) metadataRows.selectExpr("record_count").collect();
        }
        if (nullValueCount == null) {
          nullValueCount = (Row[]) metadataRows.selectExpr("null_value_counts").collect();
        }
        long count = 0;
        for (int j = 0; j < recordCount.length; j++) {
          count += recordCount[j].getLong(0);
        }
        int index = indexInTableSchema(fields[i]);
        long numOfNulls = getNullValueCount(nullValueCount, index + 1);
        valuesInSparkInternalRow.add(count - numOfNulls);
      }
    }
    rows[0] = InternalRow.fromSeq(JavaConverters.asScalaBuffer(valuesInSparkInternalRow).toSeq());

    return rows;
  }

  private int indexInTableSchema(StructField field) {
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
    throw new RuntimeException(
        "push down aggregate failed, can't find this aggregate col in table schema.");
  }

  private long getNullValueCount(Row[] nullValueCount, int index) {
    long numOfNlls = 0L;
    for (int i = 0; i < nullValueCount.length; i++) {
      Map<Integer, Long> map = nullValueCount[i].getJavaMap(0);
      Long value = map.get(index);
      numOfNlls += value;
    }
    return numOfNlls;
  }

  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "MethodLength"})
  private Object getMinOrMax(Row[] rows, int index, StructField field, boolean isMin) {
    Object result = null;
    boolean isString = false;
    boolean isBinary = false;
    boolean isDecimal = false;

    for (int i = 0; i < rows.length; i++) {
      Map<Integer, byte[]> map = rows[i].getJavaMap(0);
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
            throw new RuntimeException("Data type is not supported in push down min()");
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

  @Override
  public StructType readSchema() {
    return aggregateSchema;
  }

  @Override
  public String description() {
    String aggregates =
        aggregateExpressions.stream().map(Spark3Util::describe).collect(Collectors.joining(", "));
    return String.format("%s [filters=%s]", table, aggregates);
  }
}
