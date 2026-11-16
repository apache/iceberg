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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests to verify that streaming checkpoints always use HadoopFileIO and never use the table's
 * FileIO implementation.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestStreamingCheckpointHadoopIO extends TestBaseWithCatalog {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        "testtableio",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hadoop",
            CatalogProperties.FILE_IO_IMPL,
            TrackingFileIO.class.getName(),
            "cache-enabled",
            "false")
      }
    };
  }

  @AfterEach
  public void stopStreams() throws TimeoutException {
    for (StreamingQuery query : spark.streams().active()) {
      query.stop();
    }
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    TrackingFileIO.reset();
  }

  @TestTemplate
  public void testCheckpointsUseHadoopIONotTableIO() throws Exception {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    // Use nested checkpoint path to verify parent directory creation
    File checkpointDir = new File(temp.toFile(), "nested/checkpoint");
    TrackingFileIO.reset();

    // Run streaming query with checkpoints
    StreamingQuery query =
        spark
            .readStream()
            .format("iceberg")
            .load(tableName)
            .writeStream()
            .format("console")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();

    query.processAllAvailable();
    query.stop();

    // Verify TrackingFileIO (table's FileIO) was NOT used for checkpoint operations
    assertThat(TrackingFileIO.wasUsed())
        .as("HadoopFileIO should be used for checkpoints, not table's FileIO")
        .isFalse();

    // Verify checkpoint files were actually created using HadoopFileIO
    assertThat(new File(checkpointDir, "offsets/0")).exists().isFile();
  }

  /**
   * A FileIO that tracks whether it was used for checkpoint operations. This allows us to verify
   * that the table's FileIO is NOT being used for checkpoints.
   */
  public static class TrackingFileIO implements FileIO {
    private static final String CHECKPOINT_OFFSETS_PATH = "/offsets/";
    private static final AtomicBoolean USED = new AtomicBoolean(false);
    private FileIO delegate;

    public static void reset() {
      USED.set(false);
    }

    public static boolean wasUsed() {
      return USED.get();
    }

    @Override
    public InputFile newInputFile(String path) {
      if (path.contains(CHECKPOINT_OFFSETS_PATH)) {
        USED.set(true);
      }
      return delegate.newInputFile(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      if (path.contains(CHECKPOINT_OFFSETS_PATH)) {
        USED.set(true);
      }
      return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      delegate.deleteFile(path);
    }

    @Override
    public void initialize(Map<String, String> properties) {
      this.delegate = new HadoopFileIO();
      this.delegate.initialize(properties);
    }

    @Override
    public void close() {
      if (delegate != null) {
        delegate.close();
      }
    }
  }
}
