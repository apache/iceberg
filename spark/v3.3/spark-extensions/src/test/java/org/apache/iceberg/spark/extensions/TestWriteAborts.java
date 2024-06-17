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
package org.apache.iceberg.spark.extensions;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

public class TestWriteAborts extends SparkExtensionsTestBase {

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "testhive",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            CatalogProperties.FILE_IO_IMPL,
            CustomFileIO.class.getName(),
            "default-namespace",
            "default")
      },
      {
        "testhivebulk",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            CatalogProperties.FILE_IO_IMPL,
            CustomBulkFileIO.class.getName(),
            "default-namespace",
            "default")
      }
    };
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  public TestWriteAborts(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testBatchAppend() throws Exception {
    String dataLocation = temp.newFolder().toString();

    sql(
        "CREATE TABLE %s (id INT, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (data)"
            + "TBLPROPERTIES ('%s' '%s')",
        tableName, TableProperties.WRITE_DATA_LOCATION, dataLocation);

    List<SimpleRecord> records =
        ImmutableList.of(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "a"),
            new SimpleRecord(4, "b"));
    Dataset<Row> inputDF = spark.createDataFrame(records, SimpleRecord.class);

    Assertions.assertThatThrownBy(
            () -> {
              try {
                // incoming records are not ordered by partitions so the job must fail
                inputDF
                    .coalesce(1)
                    .sortWithinPartitions("id")
                    .writeTo(tableName)
                    .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
                    .append();
              } catch (NoSuchTableException e) {
                throw new RuntimeException(e);
              }
            })
        .as("Write must fail")
        .isInstanceOf(SparkException.class)
        .hasMessageContaining("Writing job aborted");

    assertEquals("Should be no records", sql("SELECT * FROM %s", tableName), ImmutableList.of());

    assertEquals(
        "Should be no orphan data files",
        ImmutableList.of(),
        sql(
            "CALL %s.system.remove_orphan_files(table => '%s', older_than => %dL, location => '%s')",
            catalogName, tableName, System.currentTimeMillis() + 5000, dataLocation));
  }

  public static class CustomFileIO implements FileIO {

    private final FileIO delegate = new HadoopFileIO(new Configuration());

    public CustomFileIO() {}

    protected FileIO delegate() {
      return delegate;
    }

    @Override
    public InputFile newInputFile(String path) {
      return delegate.newInputFile(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      delegate.deleteFile(path);
    }

    @Override
    public Map<String, String> properties() {
      return delegate.properties();
    }

    @Override
    public void initialize(Map<String, String> properties) {
      delegate.initialize(properties);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  public static class CustomBulkFileIO extends CustomFileIO implements SupportsBulkOperations {

    public CustomBulkFileIO() {}

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException("Only bulk deletes are supported");
    }

    @Override
    public void deleteFiles(Iterable<String> paths) throws BulkDeletionFailureException {
      for (String path : paths) {
        delegate().deleteFile(path);
      }
    }
  }
}
