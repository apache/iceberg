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

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.struct;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteIndexTable;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

class SparkJoinedBinPackDataRewriter extends SparkBinPackDataRewriter {

  private DeleteIndexTable deleteIndexTable;

  SparkJoinedBinPackDataRewriter(SparkSession spark, Table table) {
    super(spark, table);
  }

  public void setDeleteIndexTable(DeleteIndexTable that) {
    this.deleteIndexTable = that;
  }

  private List<String> tableFields(List<Integer> fieldIds) {
    return fieldIds.stream()
        .map(
            fdId -> {
              return table().schema().findField(fdId).name();
            })
        .collect(Collectors.toList());
  }

  private Pair<Dataset<Row>, Dataset<Row>> deleteTable(FileScanTask task) {
    Pair<Integer, StructLike> partitioniPair =
        Pair.of(task.file().specId(), task.file().partition());
    Object resultCandidate = deleteIndexTable.getDeleteTable(partitioniPair);
    if (resultCandidate != null) {
      return (Pair<Dataset<Row>, Dataset<Row>>) resultCandidate;
    }

    Map<String, Long> deletePath2SeqMap = Maps.newHashMap();
    Map<String, List<String>> eqPath2FieldsMap = Maps.newHashMap();
    List<String> posDeleteFiles = Lists.newArrayList();
    List<String> eqDeleteFiles = Lists.newArrayList();

    Pair<long[], DeleteFile[]> partitionDeletePair =
        deleteIndexTable.partitionDeleteEntries(partitioniPair);
    long[] partitionDeleteSeqs;
    DeleteFile[] partitionDeleteFiles;
    if (partitionDeletePair == null) {
      partitionDeleteSeqs = new long[] {};
      partitionDeleteFiles = new DeleteFile[] {};
    } else {
      partitionDeleteSeqs = partitionDeletePair.first();
      partitionDeleteFiles = partitionDeletePair.second();
    }

    for (int i = 0; i < partitionDeleteFiles.length; i += 1) {
      long deleteApplySeq = partitionDeleteSeqs[i];
      DeleteFile deleteFile = partitionDeleteFiles[i];
      String fileRawPath = (deleteFile.path().toString());
      String filePath = Paths.get(fileRawPath).toUri().normalize().toString();
      deletePath2SeqMap.put(filePath, deleteApplySeq);
      if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
        eqPath2FieldsMap.put(filePath, tableFields(deleteFile.equalityFieldIds()));
        eqDeleteFiles.add(fileRawPath);
      } else {
        posDeleteFiles.add(fileRawPath);
      }
    }

    Pair<long[], DeleteFile[]> globalDeletePair = deleteIndexTable.globalDeleteEntries();
    long[] globalDeleteSeqs;
    DeleteFile[] globalDeleteFiles;
    if (globalDeletePair == null) {
      globalDeleteSeqs = new long[] {};
      globalDeleteFiles = new DeleteFile[] {};
    } else {
      globalDeleteSeqs = globalDeletePair.first();
      globalDeleteFiles = globalDeletePair.second();
    }

    for (int i = 0; i < globalDeleteFiles.length; i += 1) {
      long deleteApplySeq = globalDeleteSeqs[i];
      DeleteFile deleteFile = globalDeleteFiles[i];
      String fileRawPath = (deleteFile.path().toString());
      String filePath = Paths.get(fileRawPath).toUri().normalize().toString();
      deletePath2SeqMap.put(filePath, deleteApplySeq);
      if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
        eqPath2FieldsMap.put(filePath, tableFields(deleteFile.equalityFieldIds()));
        eqDeleteFiles.add(fileRawPath);
      } else {
        posDeleteFiles.add(fileRawPath);
      }
    }

    Dataset<Row> posDF;
    if (posDeleteFiles.isEmpty()) {
      posDF = null;
    } else {
      posDF =
          spark()
              .read()
              .format("parquet")
              .load(posDeleteFiles.toArray(new String[0]))
              .withColumn("pos_file_path", callUDF("normalize_path", functions.input_file_name()))
              .withColumn("data_file_path", callUDF("normalize_path", col("file_path")));
      posDF =
          posDF.withColumn(
              "seq",
              functions
                  .udf(
                      (String filePath) -> {
                        return deletePath2SeqMap.get(filePath);
                      },
                      DataTypes.LongType)
                  .apply(posDF.col("pos_file_path")));
      posDF.cache();
    }

    Dataset<Row> eqDF;

    if (eqDeleteFiles.isEmpty()) {
      eqDF = null;
    } else {
      eqDF =
          spark()
              .read()
              .format("parquet")
              .load(eqDeleteFiles.toArray(new String[0]))
              .withColumn("eq_file_path", callUDF("normalize_path", functions.input_file_name()));
      eqDF =
          eqDF.withColumn(
              "seq",
              functions
                  .udf(
                      (String filePath) -> {
                        return deletePath2SeqMap.get(filePath);
                      },
                      DataTypes.LongType)
                  .apply(eqDF.col("eq_file_path")));

      eqDF =
          eqDF.withColumn(
              "eqFields",
              functions
                  .udf(
                      (String filePath) -> {
                        return eqPath2FieldsMap.get(filePath);
                      },
                      DataTypes.createArrayType(DataTypes.StringType))
                  .apply(eqDF.col("eq_file_path")));

      List<Column> eqDFColumns =
          Arrays.stream(eqDF.columns()).map(functions::col).collect(Collectors.toList());
      eqDF = eqDF.withColumn("row", struct(eqDFColumns.toArray(new Column[0])));

      eqDF.cache();
    }

    Pair<Dataset<Row>, Dataset<Row>> result = Pair.of(posDF, eqDF);
    deleteIndexTable.putDeleteTable(partitioniPair, result);
    return result;
  }

  @Override
  protected void doRewrite(String groupId, List<FileScanTask> group) {
    // read the files packing them into splits of the required size
    Map<String, Long> dataPath2SeqMap = Maps.newHashMap();
    group.forEach(
        fileScanTask -> {
          String fileRawPath = fileScanTask.file().path().toString();
          String filePath = Paths.get(fileRawPath).toUri().normalize().toString();
          dataPath2SeqMap.put(filePath, fileScanTask.file().dataSequenceNumber());
        });

    // Define the UDF to normalize file paths

    UDF1<String, String> normalizePathUdf = path -> Paths.get(path).toUri().normalize().toString();

    UDF2<Row, Row, Boolean> eqdUdf =
        (eqRow, scanRow) -> {
          List<String> eqFields = eqRow.getList(eqRow.fieldIndex("eqFields"));
          for (String field : eqFields) {
            if (!scanRow.getAs(field).equals(eqRow.getAs(field))) {
              return false;
            }
          }
          return true;
        };

    spark().udf().register("eqd_udf", eqdUdf, DataTypes.BooleanType);
    spark().udf().register("normalize_path", normalizePathUdf, DataTypes.StringType);

    Dataset<Row> scanDF =
        spark()
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
            .option(SparkReadOptions.SPLIT_SIZE, splitSize(inputSize(group)))
            .option(SparkReadOptions.FILE_OPEN_COST, "0")
            .load(groupId);

    String[] targetColumns = scanDF.columns();
    for (int i = 0; i < targetColumns.length; i++) {
      targetColumns[i] = "s." + targetColumns[i];
    }

    List<Column> scanDFColumns =
        Arrays.stream(scanDF.columns()).map(functions::col).collect(Collectors.toList());
    scanDF = scanDF.withColumn("row", struct(scanDFColumns.toArray(new Column[0])));

    scanDF = scanDF.withColumn("file_path", callUDF("normalize_path", functions.input_file_name()));

    scanDF =
        scanDF.withColumn(
            "seq",
            functions
                .udf(
                    (String filePath) -> {
                      return dataPath2SeqMap.get(filePath);
                    },
                    DataTypes.LongType)
                .apply(scanDF.col("file_path")));

    scanDF =
        scanDF.withColumn(
            "row_number",
            functions
                .row_number()
                .over(Window.partitionBy("file_path").orderBy(functions.lit("")))
                .minus(1));

    // now I have a Dataset<Row> `scanDF` with its schema columns and file_path: String, seq: Long,
    // row_number: Long
    // a Dataset<Row> `posDF` with columns: file_path: String, pos: Integer, seq: Long
    // a Dataset<Row> `eqDF`  with columns: file_path: String, seq: Long, eqFields: Array<String>
    // I want to perform a three table join:
    // scanDF s left join posDF p on s.seq <= p.seq and s.file_path = p.file_path and s.row_number =
    // p.pos
    // scanDF s left join eqDF e on s.seq <= e.seq and a customized udf eqd_udf(e, s) = true
    //    where only when all fields in e.eqFields match on e and s rows
    // filter on p.seq = null, meaning only unmatched join rows to posDF will retain
    // filter on e.seq = null, meaning only unmatched join rows to eqDF will retain

    Pair<Dataset<Row>, Dataset<Row>> deleteTablePair = deleteTable(group.get(0));
    Dataset<Row> posDF = deleteTablePair.first();
    Dataset<Row> eqDF = deleteTablePair.second();

    Dataset<Row> resultDF = null;

    if (posDF != null && eqDF != null) {
      resultDF =
          scanDF
              .alias("s")
              .join(
                  posDF.alias("p"),
                  expr(
                      "s.seq <= p.seq AND s.file_path = p.data_file_path AND s.row_number = p.pos"),
                  "left_outer")
              .join(
                  eqDF.alias("e"),
                  expr("s.seq <= e.seq AND eqd_udf(e.row, s.row) = true"),
                  "left_outer")
              .filter(col("p.seq").isNull())
              .filter(col("e.seq").isNull())
              .selectExpr(targetColumns);
    } else if (posDF == null && eqDF != null) {
      resultDF =
          scanDF
              .alias("s")
              .join(
                  eqDF.alias("e"),
                  expr("s.seq <= e.seq AND eqd_udf(e.row, s.row) = true"),
                  "left_outer")
              .filter(col("e.seq").isNull())
              .selectExpr(targetColumns);
    } else if (posDF != null && eqDF == null) {
      resultDF =
          scanDF
              .alias("s")
              .join(
                  posDF.alias("p"),
                  expr(
                      "s.seq <= p.seq AND s.file_path = p.data_file_path AND s.row_number = p.pos"),
                  "left_outer")
              .filter(col("p.seq").isNull())
              .selectExpr(targetColumns);
    } else if (posDF == null && eqDF == null) {
      resultDF = scanDF.alias("s").selectExpr(targetColumns);
    }

    resultDF
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
        .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, writeMaxFileSize())
        .option(SparkWriteOptions.DISTRIBUTION_MODE, distributionMode(group).modeName())
        .mode("append")
        .save(groupId);
  }

  // invoke a shuffle if the original spec does not match the output spec
  private DistributionMode distributionMode(List<FileScanTask> group) {
    boolean requiresRepartition = !group.get(0).spec().equals(table().spec());
    return requiresRepartition ? DistributionMode.RANGE : DistributionMode.NONE;
  }
}
