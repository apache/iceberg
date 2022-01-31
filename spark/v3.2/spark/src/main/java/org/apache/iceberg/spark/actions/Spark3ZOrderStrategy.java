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

package org.apache.iceberg.spark.actions;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.FileScanTaskSetManager;
import org.apache.iceberg.spark.SparkDistributionAndOrderingUtil;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.SortOrderUtil;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.internal.SQLConf;
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
import org.apache.spark.sql.types.TimestampType;
import scala.collection.Seq;

public class Spark3ZOrderStrategy extends Spark3SortStrategy {
  private static final String Z_COLUMN = "ICEZVALUE";
  private static final Schema Z_SCHEMA = new Schema(NestedField.required(0, Z_COLUMN, Types.BinaryType.get()));
  private static final org.apache.iceberg.SortOrder Z_SORT_ORDER = org.apache.iceberg.SortOrder.builderFor(Z_SCHEMA)
      .sortBy(Z_COLUMN, SortDirection.ASC, NullOrder.NULLS_LAST)
      .build();
  private static final int STRING_KEY_LENGTH = 60;

  private final List<String> zOrderColNames;
  private final FileScanTaskSetManager manager = FileScanTaskSetManager.get();
  private final FileRewriteCoordinator rewriteCoordinator = FileRewriteCoordinator.get();

  public Spark3ZOrderStrategy(Table table, SparkSession spark, List<String> zOrderColNames) {
    super(table, spark);

    Stream<String> identityPartitionColumns = table.spec().fields().stream()
        .filter(f -> f.transform().isIdentity())
        .map(PartitionField::name);
    List<String> partZOrderCols = identityPartitionColumns
        .filter(zOrderColNames::contains)
        .collect(Collectors.toList());
    Preconditions.checkArgument(partZOrderCols.isEmpty(),
        "Cannot ZOrder on an Identity partition column as these values are constant within a partition, " +
            "ZOrdering requested on %s",
        partZOrderCols);

    this.zOrderColNames = zOrderColNames;
  }

  @Override
  public String name() {
    return "Z-ORDER";
  }

  @Override
  protected void validateOptions() {
    // TODO implement Zorder Strategy in API Module
    return;
  }

  @Override
  public Set<DataFile> rewriteFiles(List<FileScanTask> filesToRewrite) {
    String groupID = UUID.randomUUID().toString();
    boolean requiresRepartition = !filesToRewrite.get(0).spec().equals(table().spec());

    SortOrder[] ordering;
    if (requiresRepartition) {
      ordering = SparkDistributionAndOrderingUtil.convert(SortOrderUtil.buildSortOrder(table(), sortOrder()));
    } else {
      ordering = SparkDistributionAndOrderingUtil.convert(sortOrder());
    }

    Distribution distribution = Distributions.ordered(ordering);

    try {
      manager.stageTasks(table(), groupID, filesToRewrite);

      // Disable Adaptive Query Execution as this may change the output partitioning of our write
      SparkSession cloneSession = spark().cloneSession();
      cloneSession.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), false);

      // Reset Shuffle Partitions for our sort
      long numOutputFiles = numOutputFiles((long) (inputFileSize(filesToRewrite) * sizeEstimateMultiple()));
      cloneSession.conf().set(SQLConf.SHUFFLE_PARTITIONS().key(), Math.max(1, numOutputFiles));

      Dataset<Row> scanDF = cloneSession.read().format("iceberg")
          .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, groupID)
          .load(table().name());

      Column[] originalColumns = Arrays.stream(scanDF.schema().names())
          .map(n -> functions.col(n))
          .toArray(Column[]::new);

      List<StructField> zOrderColumns =  zOrderColNames.stream()
          .map(scanDF.schema()::apply)
          .collect(Collectors.toList());

      Column zvalueArray = functions.array(zOrderColumns.stream().map(colStruct ->
            SparkZOrder.sortedLexicographically(functions.col(colStruct.name()), colStruct.dataType())
          ).toArray(Column[]::new));

      Dataset<Row> zvalueDF = scanDF.withColumn(Z_COLUMN, SparkZOrder.interleaveBytes(zvalueArray));

      SQLConf sqlConf = cloneSession.sessionState().conf();
      LogicalPlan sortPlan = sortPlan(distribution, ordering, zvalueDF.logicalPlan(), sqlConf);
      Dataset<Row> sortedDf = new Dataset<>(cloneSession, sortPlan, zvalueDF.encoder());
      sortedDf
          .select(originalColumns)
          .write()
          .format("iceberg")
          .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupID)
          .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, writeMaxFileSize())
          .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
          .mode("append")
          .save(table().name());

      return rewriteCoordinator.fetchNewDataFiles(table(), groupID);
    } finally {
      manager.removeTasks(table(), groupID);
      rewriteCoordinator.clearRewrite(table(), groupID);
    }
  }

  @Override
  protected org.apache.iceberg.SortOrder sortOrder() {
    return Z_SORT_ORDER;
  }

  static class SparkZOrder {

    static byte[] interleaveBits(Seq<byte[]> scalaBinary) {
      byte[][] columnsBinary = scala.collection.JavaConverters.seqAsJavaList(scalaBinary)
          .toArray(new byte[scalaBinary.size()][]);
      return ZOrderByteUtils.interleaveBits(columnsBinary);
    }

    private static UserDefinedFunction intToOrderedBytesUDF() {
      return functions.udf((Integer value) -> {
        if (value == null) {
          return null;
        }
        return ZOrderByteUtils.intToOrderedBytes(value);
      }, DataTypes.BinaryType)
          .withName("INT-LEXICAL-BYTES");
    }

    private static UserDefinedFunction longToOrderedBytesUDF() {
      return functions.udf((Long value) -> {
            if (value == null) {
              return null;
            }
            return ZOrderByteUtils.longToOrderBytes(value);
          }, DataTypes.BinaryType)
          .withName("LONG-LEXICAL-BYTES");
    }

    private static UserDefinedFunction floatToOrderedBytesUDF() {
      return functions.udf((Float value) -> {
            if (value == null) {
              return null;
            }
            return ZOrderByteUtils.floatToOrderedBytes(value);
          }, DataTypes.BinaryType)
          .withName("FLOAT-LEXICAL-BYTES");
    }

    private static UserDefinedFunction doubleToOrderedBytesUDF() {
      return functions.udf((Double value) -> {
            if (value == null) {
              return null;
            }
            return ZOrderByteUtils.doubleToOrderedBytes(value);
          }, DataTypes.BinaryType)
          .withName("DOUBLE-LEXICAL-BYTES");
    }

    private static UserDefinedFunction stringToOrderedBytesUDF() {
      return functions.udf((String value) -> {
            if (value == null) {
              return null;
            }
            return ZOrderByteUtils.stringToOrderedBytes(value);
          }, DataTypes.BinaryType)
          .withName("STRING-LEXICAL-BYTES");
    }

    private static final UserDefinedFunction INTERLEAVE_UDF =
        functions.udf((Seq<byte[]> arrayBinary) -> interleaveBits(arrayBinary), DataTypes.BinaryType)
            .withName("INTERLEAVE_BYTES");

    static Column interleaveBytes(Column arrayBinary) {
      return INTERLEAVE_UDF.apply(arrayBinary);
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    static Column sortedLexicographically(Column column, DataType type) {
      if (type instanceof ByteType) {
        return column.cast(DataTypes.BinaryType);
      } else if (type instanceof ShortType) {
        return getLexicalBytesIntLike(2).apply(column.cast(DataTypes.BinaryType));
      } else if (type instanceof IntegerType) {
        return getLexicalBytesIntLike(4).apply(column.cast(DataTypes.BinaryType));
      } else if (type instanceof LongType) {
        return getLexicalBytesIntLike(8).apply(column.cast(DataTypes.BinaryType));
      } else if (type instanceof FloatType) {
        return getLexicalBytesFloatLike(4).apply(FLOAT_TO_BYTES.apply(column));
      } else if (type instanceof DoubleType) {
        return getLexicalBytesFloatLike(8).apply(DOUBLE_TO_BYTES.apply(column));
      } else if (type instanceof StringType) {
        return getLexicalBytesUTF8Like(STRING_KEY_LENGTH).apply(column.cast(DataTypes.BinaryType));
      } else if (type instanceof BinaryType) {
        return getLexicalBytesUTF8Like(STRING_KEY_LENGTH).apply(column);
      } else if (type instanceof BooleanType) {
        return getLexicalBytesUTF8Like(1).apply(column.cast(DataTypes.BinaryType));
      } else if (type instanceof TimestampType) {
        return getLexicalBytesIntLike(8).apply(column.cast(DataTypes.LongType).cast(DataTypes.BinaryType));
      } else if (type instanceof DateType) {
        return getLexicalBytesIntLike(8).apply(column.cast(DataTypes.LongType).cast(DataTypes.BinaryType));
      } else {
        throw new IllegalArgumentException(
            String.format("Cannot use column %s of type %s in ZOrdering, the type is unsupported",
                column, type));
      }
    }
  }
}
