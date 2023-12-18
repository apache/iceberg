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
package org.apache.iceberg.mr.hive.vector;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.ql.io.orc.VectorizedOrcInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.VectorizedParquetInputFormat;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mr.mapred.MapredIcebergInputFormat;
import org.apache.iceberg.orc.VectorizedReadUtils;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.orc.impl.OrcTail;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;

/**
 * Utility class to create vectorized readers for Hive. As per the file format of the task, it will
 * create a matching vectorized record reader that is already implemented in Hive. It will also do
 * some tweaks on the produced vectors for Iceberg's use e.g. partition column handling.
 */
public class HiveVectorizedReader {

  private HiveVectorizedReader() {}

  public static <D> CloseableIterable<D> reader(
      InputFile inputFile,
      FileScanTask task,
      Map<Integer, ?> idToConstant,
      TaskAttemptContext context) {
    JobConf job = (JobConf) context.getConfiguration();
    Path path = new Path(inputFile.location());
    FileFormat format = task.file().format();
    Reporter reporter =
        ((MapredIcebergInputFormat.CompatibilityTaskAttemptContextImpl) context)
            .getLegacyReporter();

    // Hive by default requires partition columns to be read too. This is not required for identity
    // partition
    // columns, as we will add this as constants later.

    int[] partitionColIndices = null;
    Object[] partitionValues = null;
    PartitionSpec partitionSpec = task.spec();

    if (!partitionSpec.isUnpartitioned()) {
      List<Integer> readColumnIds = ColumnProjectionUtils.getReadColumnIDs(job);

      List<PartitionField> fields = partitionSpec.fields();
      List<Integer> partitionColIndicesList = Lists.newLinkedList();
      List<Object> partitionValuesList = Lists.newLinkedList();

      for (PartitionField field : fields) {
        if (field.transform().isIdentity()) {
          // Skip reading identity partition columns from source file...
          int hiveColIndex = field.sourceId() - 1;
          readColumnIds.remove((Integer) hiveColIndex);

          // ...and use the corresponding constant value instead
          partitionColIndicesList.add(hiveColIndex);
          partitionValuesList.add(idToConstant.get(field.sourceId()));
        }
      }

      partitionColIndices = partitionColIndicesList.stream().mapToInt(Integer::intValue).toArray();
      partitionValues = partitionValuesList.toArray(new Object[0]);

      ColumnProjectionUtils.setReadColumns(job, readColumnIds);
    }

    try {

      long start = task.start();
      long length = task.length();

      RecordReader<NullWritable, VectorizedRowBatch> recordReader = null;

      switch (format) {
        case ORC:
          recordReader = orcRecordReader(job, reporter, task, inputFile, path, start, length);
          break;
        case PARQUET:
          recordReader = parquetRecordReader(job, reporter, task, path, start, length);
          break;

        default:
          throw new UnsupportedOperationException(
              "Vectorized Hive reading unimplemented for format: " + format);
      }

      return createVectorizedRowBatchIterable(
          recordReader, job, partitionColIndices, partitionValues);

    } catch (IOException ioe) {
      throw new RuntimeException("Error creating vectorized record reader for " + inputFile, ioe);
    }
  }

  private static RecordReader<NullWritable, VectorizedRowBatch> orcRecordReader(
      JobConf job,
      Reporter reporter,
      FileScanTask task,
      InputFile inputFile,
      Path path,
      long start,
      long length)
      throws IOException {
    // Metadata information has to be passed along in the OrcSplit. Without specifying this, the
    // vectorized
    // reader will assume that the ORC file ends at the task's start + length, and might fail
    // reading the tail..
    OrcTail orcTail = VectorizedReadUtils.getOrcTail(inputFile, job);

    InputSplit split =
        new OrcSplit(
            path,
            null,
            start,
            length,
            (String[]) null,
            orcTail,
            false,
            false,
            Lists.newArrayList(),
            0,
            task.length(),
            path.getParent());
    return new VectorizedOrcInputFormat().getRecordReader(split, job, reporter);
  }

  private static RecordReader<NullWritable, VectorizedRowBatch> parquetRecordReader(
      JobConf job, Reporter reporter, FileScanTask task, Path path, long start, long length)
      throws IOException {
    InputSplit split = new FileSplit(path, start, length, job);
    VectorizedParquetInputFormat inputFormat = new VectorizedParquetInputFormat();

    MessageType fileSchema = ParquetFileReader.readFooter(job, path).getFileMetaData().getSchema();
    MessageType typeWithIds = null;
    Schema expectedSchema = task.spec().schema();

    if (ParquetSchemaUtil.hasIds(fileSchema)) {
      typeWithIds = ParquetSchemaUtil.pruneColumns(fileSchema, expectedSchema);
    } else {
      typeWithIds =
          ParquetSchemaUtil.pruneColumnsFallback(
              ParquetSchemaUtil.addFallbackIds(fileSchema), expectedSchema);
    }

    ParquetSchemaFieldNameVisitor psv = new ParquetSchemaFieldNameVisitor(fileSchema);
    TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), typeWithIds, psv);
    job.set(IOConstants.COLUMNS, psv.retrieveColumnNameList());

    return inputFormat.getRecordReader(split, job, reporter);
  }

  private static <D> CloseableIterable<D> createVectorizedRowBatchIterable(
      RecordReader<NullWritable, VectorizedRowBatch> hiveRecordReader,
      JobConf job,
      int[] partitionColIndices,
      Object[] partitionValues) {

    VectorizedRowBatchIterator iterator =
        new VectorizedRowBatchIterator(hiveRecordReader, job, partitionColIndices, partitionValues);

    return new CloseableIterable<D>() {

      @Override
      public CloseableIterator iterator() {
        return iterator;
      }

      @Override
      public void close() throws IOException {
        iterator.close();
      }
    };
  }
}
