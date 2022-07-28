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
package org.apache.iceberg.spark.source.parquet;

import java.io.IOException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.spark.source.IcebergSourceDeleteBenchmark;
import org.openjdk.jmh.annotations.Param;

/**
 * A benchmark that evaluates the non-vectorized read and vectorized read with pos-delete in the
 * Spark data source for Iceberg.
 *
 * <p>This class uses a dataset with a flat schema. To run this benchmark for spark-3.3: <code>
 *   ./gradlew -DsparkVersions=3.3 :iceberg-spark:iceberg-spark-3.3:jmh \
 *       -PjmhIncludeRegex=IcebergSourceParquetMultiDeleteFileBenchmark \
 *       -PjmhOutputPath=benchmark/iceberg-source-parquet-multi-delete-file-benchmark-result.txt
 * </code>
 */
public class IcebergSourceParquetMultiDeleteFileBenchmark extends IcebergSourceDeleteBenchmark {
  @Param({"1", "2", "5", "10"})
  private int numDeleteFile;

  @Override
  protected void appendData() throws IOException {
    for (int fileNum = 1; fileNum <= NUM_FILES; fileNum++) {
      writeData(fileNum);

      table().refresh();
      for (DataFile file : table().currentSnapshot().addedDataFiles(table().io())) {
        writePosDeletes(file.path(), NUM_ROWS, 0.25, numDeleteFile);
      }
    }
  }

  @Override
  protected FileFormat fileFormat() {
    return FileFormat.PARQUET;
  }
}
