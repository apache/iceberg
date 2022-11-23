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
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.AggregateUtil;
import org.apache.iceberg.expressions.BoundAggregate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.Decimal;
import scala.collection.JavaConverters;

/** Helper methods for working with Spark aggregate push down. */
public class SparkPushedDownAggregateUtil {

  private SparkPushedDownAggregateUtil() {}

  public static boolean metricsModeSupportsAggregatePushDown(
      Table table, List<Expression> aggregates) {
    MetricsConfig config = MetricsConfig.forTable(table);
    for (Expression aggregate : aggregates) {
      String colName = AggregateUtil.getAggregateColumnName(aggregate);
      if (!colName.equals("*")) {
        MetricsModes.MetricsMode mode = config.columnMode(colName);
        if (mode.toString().equals("none")) {
          return false;
        } else if (mode.toString().equals("counts")) {
          if (aggregate.op() == Expression.Operation.MAX
              || aggregate.op() == Expression.Operation.MIN) {
            return false;
          }
        } else if (mode.toString().contains("truncate")) {
          if (AggregateUtil.getAggregateType(aggregate).typeId() == Type.TypeID.STRING) {
            if (aggregate.op() == Expression.Operation.MAX
                || aggregate.op() == Expression.Operation.MIN) {
              return false;
            }
          }
        }
      }
    }

    return true;
  }

  public static InternalRow[] constructInternalRowForPushedDownAggregate(
      SparkSession spark, Table table, List<Expression> aggregates, List<Integer> indexInTable) {
    List<Object> valuesInSparkInternalRow = Lists.newArrayList();
    Row[] row = SparkPushedDownAggregateUtil.getStatisticRow(spark, table);
    for (int index = 0; index < aggregates.size(); index++) {
      Expression aggregate = aggregates.get(index);
      Type type = AggregateUtil.getAggregateType(aggregate);
      valuesInSparkInternalRow.add(
          SparkPushedDownAggregateUtil.getAggregateValue(
              aggregate, row, indexInTable.get(index), type));
    }

    InternalRow[] rows = new InternalRow[1];
    rows[0] = InternalRow.fromSeq(JavaConverters.asScalaBuffer(valuesInSparkInternalRow).toSeq());
    return rows;
  }

  public static Row[] getStatisticRow(SparkSession spark, Table table) {
    Dataset<Row> metadataRows =
        SparkTableUtil.loadMetadataTable(spark, table, MetadataTableType.DATA_FILES);
    Dataset dataset =
        metadataRows.selectExpr(
            "lower_bounds", "upper_bounds", "record_count", "null_value_counts");
    return (Row[]) dataset.collect();
  }

  public static <T> T getAggregateValue(
      Expression expr, Row[] statisticRows, int index, Type type) {
    return (T)
        ExpressionVisitors.visit(expr, new AggregateValueVisitor(statisticRows, index, type));
  }

  private static class AggregateValueVisitor<T>
      extends ExpressionVisitors.BoundExpressionVisitor<T> {

    static final int LOWER_BOUNDS_COLUMN_INDEX = 0;
    static final int UPPER_BOUNDS_COLUMN_INDEX = 1;
    static final int RECORD_COUNT_COLUMN_INDEX = 2;
    static final int NULL_COUNT_COLUMN_INDEX = 3;

    // statisticRows: "lower_bounds", "upper_bounds", "record_count", "null_value_counts"
    // here is one example of the rows:
    // +-----------------------------+----------------------------+------------+------------------------+
    // |lower_bounds                 |upper_bounds                |record_count|null_value_counts
    //    |
    // +-----------------------------+----------------------------+------------+------------------------+
    // |{1 -> a, 2 -> aaa, 3 -> null}|{1 -> n, 2 -> ccc, 3 -> vvv}|2           |{1 -> 0, 2 -> 0, 3
    // -> 0}|
    // |{1 -> b, 2 -> lll, 3 -> ccc} |{1 -> m, 2 -> lll, 3 -> ccc}|1           |{1 -> 0, 2 -> 0, 3
    // -> 0}|
    // |{1 -> a, 2 -> bbb, 3 -> cc}  |{1 -> v, 2 -> bbb, 3 -> cc} |1           |{1 -> 0, 2 -> 0, 3
    // -> 0}|
    // |{1 -> n, 2 -> mmm, 3 -> bbb} |{1 -> o, 2 -> nnn, 3 -> mmm}|2           |{1 -> 0, 2 -> 0, 3
    // -> 0}|
    // +-----------------------------+----------------------------+------------+------------------------+
    private final Row[] statisticRows;

    // index of this column in the original table schema
    // e.g. MAX (col2), suppose the table schema is col1, col2, col3, then this index is 1.
    // this (index + 1) is the key in the map values in lower_bounds/upper_bounds/null_value_counts
    private final int index;
    private final Type type;

    private AggregateValueVisitor(Row[] statisticRows, int index, Type type) {
      this.statisticRows = statisticRows;
      this.index = index;
      this.type = type;
    }

    public <C, R> T aggregate(BoundAggregate<C, R> agg) {
      switch (agg.op()) {
        case COUNT:
          Long count = 0L;
          for (int i = 0; i < statisticRows.length; i++) {
            count += statisticRows[i].getLong(RECORD_COUNT_COLUMN_INDEX);
          }

          Long numOfNulls = getNullValueCount();
          return (T) (Long.valueOf(count - numOfNulls));
        case COUNT_STAR:
          Long countStar = 0L;
          for (int i = 0; i < statisticRows.length; i++) {
            countStar += statisticRows[i].getLong(RECORD_COUNT_COLUMN_INDEX);
          }

          return (T) countStar;
        case MAX:
          return getMinOrMax(false);
        case MIN:
          return getMinOrMax(true);
        default:
          throw new UnsupportedOperationException("Invalid aggregate: " + agg.op());
      }
    }

    private long getNullValueCount() {
      long numOfNulls = 0L;
      for (int i = 0; i < statisticRows.length; i++) {
        Long value = (Long) statisticRows[i].getJavaMap(NULL_COUNT_COLUMN_INDEX).get(index + 1);
        numOfNulls += value;
      }

      return numOfNulls;
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength"})
    private <T> T getMinOrMax(boolean isMin) {
      T result = null;
      boolean isString = false;
      boolean isBinary = false;
      boolean isDecimal = false;
      int columIndex = LOWER_BOUNDS_COLUMN_INDEX;
      if (!isMin) {
        columIndex = UPPER_BOUNDS_COLUMN_INDEX;
      }

      for (int i = 0; i < statisticRows.length; i++) {
        byte[] valueInBytes = (byte[]) statisticRows[i].getJavaMap(columIndex).get(index + 1);
        if (valueInBytes != null) {
          switch (type.typeId()) {
            case BOOLEAN:
              boolean booleanValue =
                  Conversions.fromByteBuffer(
                      Types.BooleanType.get(), ByteBuffer.wrap(valueInBytes));
              if (isMin) {
                result = (T) Boolean.TRUE;
                if (!booleanValue) {
                  return (T) Boolean.FALSE;
                }
              } else {
                result = (T) Boolean.FALSE;
                if (booleanValue) {
                  return (T) Boolean.TRUE;
                }
              }

              return result;
            case INTEGER:
            case DATE:
              int intValue =
                  Conversions.fromByteBuffer(
                      Types.IntegerType.get(), ByteBuffer.wrap(valueInBytes));
              if (isMin) {
                if (result == null
                    || (Literal.of(intValue)).comparator().compare(intValue, (Integer) result)
                        < 0) {
                  result = (T) Integer.valueOf(intValue);
                }
              } else {
                if (result == null
                    || (Literal.of(intValue)).comparator().compare(intValue, (Integer) result)
                        > 0) {
                  result = (T) Integer.valueOf(intValue);
                }
              }
              break;
            case LONG:
            case TIME:
            case TIMESTAMP:
              long longValue =
                  Conversions.fromByteBuffer(Types.LongType.get(), ByteBuffer.wrap(valueInBytes));
              if (isMin) {
                if (result == null
                    || (Literal.of(longValue)).comparator().compare(longValue, (Long) result) < 0) {
                  result = (T) Long.valueOf(longValue);
                }
              } else {
                if (result == null
                    || (Literal.of(longValue)).comparator().compare(longValue, (Long) result) > 0) {
                  result = (T) Long.valueOf(longValue);
                }
              }
              break;
            case FLOAT:
              float fValue =
                  Conversions.fromByteBuffer(Types.FloatType.get(), ByteBuffer.wrap(valueInBytes));
              if (isMin) {
                if (result == null
                    || (Literal.of(fValue)).comparator().compare(fValue, (Float) result) < 0) {
                  result = (T) Float.valueOf(fValue);
                }
              } else {
                if (result == null
                    || (Literal.of(fValue)).comparator().compare(fValue, (Float) result) > 0) {
                  result = (T) Float.valueOf(fValue);
                }
              }
              break;
            case DOUBLE:
              double doubleValue =
                  Conversions.fromByteBuffer(Types.DoubleType.get(), ByteBuffer.wrap(valueInBytes));
              if (isMin) {
                if (result == null
                    || (Literal.of(doubleValue)).comparator().compare(doubleValue, (Double) result)
                        < 0) {
                  result = (T) Double.valueOf(doubleValue);
                }
              } else {
                if (result == null
                    || (Literal.of(doubleValue)).comparator().compare(doubleValue, (Double) result)
                        > 0) {
                  result = (T) Double.valueOf(doubleValue);
                }
              }
              break;
            case STRING:
              String stringValue =
                  Conversions.fromByteBuffer(Types.StringType.get(), ByteBuffer.wrap(valueInBytes))
                      .toString();
              if (isMin) {
                if (result == null
                    || (Literal.of(stringValue)).comparator().compare(stringValue, (String) result)
                        < 0) {
                  result = (T) stringValue;
                }
              } else {
                if (result == null
                    || (Literal.of(stringValue)).comparator().compare(stringValue, (String) result)
                        > 0) {
                  result = (T) stringValue;
                }
              }

              isString = true;
              break;
            case FIXED:
            case BINARY:
              ByteBuffer binaryValue =
                  Conversions.fromByteBuffer(Types.BinaryType.get(), ByteBuffer.wrap(valueInBytes));
              if (isMin) {
                if (result == null
                    || (Literal.of(binaryValue))
                            .comparator()
                            .compare(binaryValue, (ByteBuffer) result)
                        < 0) {
                  result = (T) binaryValue;
                }
              } else {
                if (result == null
                    || (Literal.of(binaryValue))
                            .comparator()
                            .compare(binaryValue, (ByteBuffer) result)
                        > 0) {
                  result = (T) binaryValue;
                }
              }

              isBinary = true;
              break;
            case DECIMAL:
              int precision = ((Types.DecimalType) type).precision();
              int scale = ((Types.DecimalType) type).scale();
              BigDecimal decimal =
                  Conversions.fromByteBuffer(
                      Types.DecimalType.of(precision, scale), ByteBuffer.wrap(valueInBytes));
              if (isMin) {
                if (result == null
                    || (Literal.of(decimal)).comparator().compare(decimal, (BigDecimal) result)
                        < 0) {
                  result = (T) decimal;
                }
              } else {
                if (result == null
                    || (Literal.of(decimal)).comparator().compare(decimal, (BigDecimal) result)
                        > 0) {
                  result = (T) decimal;
                }
              }

              isDecimal = true;
              break;
            default:
              throw new UnsupportedOperationException(
                  "Data type is not supported: " + type.typeId());
          }
        }
      }

      if (isString) {
        return (T) org.apache.spark.unsafe.types.UTF8String.fromString(result.toString());
      }

      if (isBinary) {
        byte[] arr = new byte[((ByteBuffer) result).remaining()];
        ((ByteBuffer) result).get(arr);
        return (T) arr;
      }

      if (isDecimal) {
        return (T) Decimal.apply(new scala.math.BigDecimal((BigDecimal) result));
      }

      return result;
    }
  }
}
