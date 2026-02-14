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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests to verify that checkpoint locations use the correct FileIO configuration, independent of
 * the table's FileIO configuration.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestCheckpointFileIO extends TestBaseWithCatalog {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        "testhive",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            CatalogProperties.FILE_IO_IMPL,
            RestrictedPathFileIO.class.getName(),
            "default-namespace",
            "default")
      },
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
    RestrictedPathFileIO.setRejectedPrefix(null);
  }

  @TestTemplate
  public void testCheckpointLocationDoesNotUseTableFileIO() throws Exception {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    // Verify the table is using RestrictedPathFileIO from the Spark catalog
    SparkCatalog sparkCatalog =
        (SparkCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());
    SparkTable sparkTable = (SparkTable) sparkCatalog.loadTable(identifier);
    Table table = sparkTable.table();
    assertThat(table.io())
        .as("Table should be using RestrictedPathFileIO from catalog configuration")
        .isInstanceOf(RestrictedPathFileIO.class);

    File checkpointDir = new File(temp.toFile(), "checkpoint");

    // Configure RestrictedPathFileIO to reject this specific checkpoint path
    // Include file:// scheme to match the actual paths being validated
    RestrictedPathFileIO.setRejectedPrefix("file:" + checkpointDir.getAbsolutePath());
    StreamingQuery query =
        spark
            .readStream()
            .format("iceberg")
            .load(tableName)
            .writeStream()
            .format("console")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();

    try {
      query.processAllAvailable();

      File offsetsDir = new File(checkpointDir, "offsets");
      assertThat(offsetsDir).exists().isDirectory();
      assertThat(new File(offsetsDir, "0")).exists().isFile();
    } finally {
      query.stop();
    }
  }

  /**
   * A FileIO implementation that rejects operations on paths with a specific prefix. This is used
   * to test that checkpoint locations don't use the table's FileIO configuration.
   */
  public static class RestrictedPathFileIO implements FileIO {
    private static String rejectedPrefix;
    private FileIO delegate;

    public static void setRejectedPrefix(String prefix) {
      rejectedPrefix = prefix;
    }

    @Override
    public InputFile newInputFile(String path) {
      validatePath(path);
      return delegate.newInputFile(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      validatePath(path);
      return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      validatePath(path);
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

    private void validatePath(String path) {
      if (rejectedPrefix != null && path.startsWith(rejectedPrefix)) {
        throw new IllegalArgumentException("RestrictedPathFileIO rejects path: " + path);
      }
    }
  }
}
