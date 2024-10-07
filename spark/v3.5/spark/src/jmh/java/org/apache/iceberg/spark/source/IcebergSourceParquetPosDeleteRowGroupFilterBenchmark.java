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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A benchmark that evaluates the non-vectorized read and vectorized read with pos-delete in the
 * Spark data source for Iceberg.
 *
 * <p>This class uses a dataset with a flat schema. To run this benchmark for spark-3.5: <code>
 *   ./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5:jmh
 *       -PjmhIncludeRegex=IcebergSourceParquetPosDeleteRowGroupFilterBenchmark.readIcebergVectorized
 *       -PjmhOutputPath=benchmark/iceberg-source-parquet-pos-delete-row-group-filter-benchmark-result.txt
 * </code>
 */
public class IcebergSourceParquetPosDeleteRowGroupFilterBenchmark
    extends IcebergSourceDeleteBenchmark {
  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergSourceParquetPosDeleteRowGroupFilterBenchmark.class);

  private static final Schema DELETE_SCHEMA =
      new Schema(MetadataColumns.DELETE_FILE_PATH, MetadataColumns.DELETE_FILE_POS);

  @Param({"2", "4", "128" /* default size */})
  private int rowGroupSizeMB;

  @Param({"3", "5"})
  private int numDataFiles;

  private DeleteFile posDeleteFile;

  @Override
  protected void appendData() throws IOException {
    for (int fileNum = 1; fileNum <= numDataFiles; fileNum++) {
      writeData(fileNum);
    }

    table()
        .updateProperties()
        .set(
            TableProperties.PATH_POS_DELETE_PARQUET_ROW_GROUP_SIZE_BYTES,
            String.valueOf(rowGroupSizeMB * 1024 * 1024))
        .commit();

    // add pos-deletes
    table().refresh();
    List<CharSequence> filePaths =
        Streams.stream(table().newScan().planFiles())
            .map(task -> task.file().path())
            .collect(Collectors.toList());

    writePosDeletes(filePaths, NUM_ROWS, 0.10);

    table().refresh();
    posDeleteFile =
        Iterables.getOnlyElement(table().currentSnapshot().addedDeleteFiles(table().io()));
    List<Long> offsets = posDeleteFile.splitOffsets();

    LOG.info(
        "row-group-size-MB: {}, position delete file size in bytes: {}, number row-groups: {}",
        rowGroupSizeMB,
        posDeleteFile.fileSizeInBytes(),
        offsets.size());
  }

  // Benchmarking the performance of reading position delete files with different number of
  // row-groups
  @Benchmark
  @Threads(1)
  public void readPositionDelete(Blackhole blackhole) {
    CloseableIterable<Record> reader =
        Parquet.read(Files.localInput(posDeleteFile.path().toString()))
            .project(DELETE_SCHEMA)
            .reuseContainers()
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(DELETE_SCHEMA, fileSchema))
            .build();

    for (Record record : reader) {
      blackhole.consume(record);
    }
  }

  @Override
  protected FileFormat fileFormat() {
    return FileFormat.PARQUET;
  }
}
