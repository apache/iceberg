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

package com.netflix.iceberg.spark.source;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.iceberg.DataFile;
import com.netflix.iceberg.Files;
import com.netflix.iceberg.ManifestReader;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.StructLike;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableMetadata;
import com.netflix.iceberg.exceptions.AlreadyExistsException;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.io.FileIO;
import com.netflix.iceberg.spark.SparkSchemaUtil;
import com.netflix.iceberg.types.Types;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestCustomIO {

  private static final Schema SCHEMA = new Schema(Types.StructType.of(
      Types.NestedField.required(100, "id", Types.IntegerType.get()),
      Types.NestedField.required(101, "data", Types.StringType.get())).fields());

  private static final PartitionSpec PARTITIONING = PartitionSpec.builderFor(SCHEMA)
      .identity("id")
      .bucket("data", 2)
      .build();

  private static final Set<UUID> GENERATED_FILE_NAMES = Sets.newConcurrentHashSet();

  private static final String METADATA_PATH_OPTION_KEY = "iceberg.test.metadata.path";
  private static final String DATA_PATH_OPTION_KEY = "iceberg.test.data.path";
  private static final String TABLE_NAME_OPTION_KEY = "iceberg.test.table.name";

  private static SparkSession spark = null;


  @BeforeClass
  public static void beforeClass() {
    TestCustomIO.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void afterClass() {
    SparkSession spark = TestCustomIO.spark;
    TestCustomIO.spark = null;
    spark.stop();
    GENERATED_FILE_NAMES.clear();
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testDataFrameWriteWithCustomIO() throws IOException {
    String tableName = String.format("test-custom-io-%s", UUID.randomUUID());
    String tablePath = temp.newFolder(tableName).getAbsolutePath();
    String metadataPath = new File(new File(tablePath), "metadata").getAbsolutePath();
    Paths.get(metadataPath).toFile().mkdirs();
    String dataPath = new File(new File(tablePath), "data").getAbsolutePath();
    Paths.get(dataPath).toFile().mkdirs();
    TestTables.TestTableOperations ops = new CustomIOTableOperations(tableName, metadataPath, dataPath);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table %s already exists at location: %s", tableName, temp);
    }
    ops.commit(null, TableMetadata.newTableMetadata(ops, SCHEMA, PARTITIONING, tablePath));
    Table table = new TestTables.TestTable(ops, tableName);
    Dataset<Row> testDataset = spark.createDataFrame(
        ImmutableList.of(
            new GenericRow(new Object[] { 10, "data1" }),
            new GenericRow(new Object[] { 10, "data2" }),
            new GenericRow(new Object[] { 11, "data3" }),
            new GenericRow(new Object[] { 12, "data4" }),
            new GenericRow(new Object[] { 13, "data5" })),
        SparkSchemaUtil.convert(SCHEMA));
    testDataset
        .write()
        .mode(SaveMode.Append)
        .option(METADATA_PATH_OPTION_KEY, metadataPath)
        .option(DATA_PATH_OPTION_KEY, dataPath)
        .option(TABLE_NAME_OPTION_KEY, tableName)
        .option("iceberg.write.format", "parquet")
        .format(CustomIOIcebergSource.class.getName())
        .save();
    Dataset<Row> writtenDataset = spark.read()
        .option(METADATA_PATH_OPTION_KEY, metadataPath)
        .option(DATA_PATH_OPTION_KEY, dataPath)
        .option(TABLE_NAME_OPTION_KEY, tableName)
        .format(CustomIOIcebergSource.class.getName())
        .load();
    Assert.assertEquals(
        "Written records were incorrect.",
        testDataset.collectAsList(),
        writtenDataset.collectAsList());
    table.refresh();
    table.currentSnapshot().manifests().forEach(manifestFile -> {
      try (ManifestReader reader = ManifestReader.read(
          table.io().newInputFile(manifestFile.path()))) {
        reader.iterator().forEachRemaining(file -> {
          Assert.assertThat(file, new BaseMatcher<DataFile>() {
            @Override
            public void describeTo(Description description) {
              description.appendText(
                  String.format("Path should start with %s and have a generated UUID.", dataPath));
            }

            @Override
            public boolean matches(Object file) {
              return file instanceof DataFile
                  && ((DataFile) file).path().toString().startsWith(dataPath)
                  && GENERATED_FILE_NAMES.contains(
                      UUID.fromString(
                          FilenameUtils.getName(((DataFile) file).path().toString())));
            }
          });
        });
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    });
  }

  public static final class CustomIOIcebergSource extends IcebergSource {

    @Override
    public String shortName() {
      return "iceberg-test-custom-io";
    }


    @Override
    protected Table findTable(DataSourceOptions options, Configuration conf) {
      String metadataPath = options.get(METADATA_PATH_OPTION_KEY)
          .orElseThrow(() -> new IllegalArgumentException(
              String.format("Expected to set the metadata path via %s.", METADATA_PATH_OPTION_KEY)));
      String dataPath = options.get(DATA_PATH_OPTION_KEY)
          .orElseThrow(() -> new IllegalArgumentException(
              String.format("Expected to set the data path via %s.", DATA_PATH_OPTION_KEY)));
      String tableName = options.get(TABLE_NAME_OPTION_KEY)
          .orElseThrow(() -> new IllegalArgumentException(
              String.format("Expected to set the table name via %s.", TABLE_NAME_OPTION_KEY)));
      TestTables.TestTableOperations ops = new CustomIOTableOperations(tableName, metadataPath, dataPath);
      ops.refresh();
      return new TestTables.TestTable(ops, tableName);
    }
  }

  private static final class CustomIOTableOperations extends TestTables.TestTableOperations {

    private final String metadataPath;
    private final String dataPath;

    CustomIOTableOperations(String tableName, String metadataPath, String dataPath) {
      super(tableName);
      this.metadataPath = metadataPath;
      this.dataPath = dataPath;
    }

    @Override
    public FileIO io() {
      return new CustomIO(metadataPath, dataPath);
    }
  }

  private static final class CustomIO implements FileIO {

    private final Map<String, String> dataFileNamesToPaths = Maps.newHashMap();
    private final String metadataPath;
    private final String dataPath;

    public CustomIO(String metadataPath, String dataPath) {
      this.metadataPath = metadataPath;
      this.dataPath = dataPath;
    }

    @Override
    public InputFile newInputFile(String path) {
      return Files.localInput(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return Files.localOutput(path);
    }

    @Override
    public void deleteFile(String path) {
      if (!new File(path).delete()) {
        throw new RuntimeIOException(
            String.format("Failed to delete file at path %s.", path));
      }
    }

    @Override
    public OutputFile newMetadataOutputFile(String fileName) {
      return newOutputFile(String.format("%s/%s", metadataPath, fileName));
    }

    @Override
    public OutputFile newDataOutputFile(
        PartitionSpec partitionSpec, StructLike partitionData, String fileName) {
      UUID randomFileName = UUID.randomUUID();
      GENERATED_FILE_NAMES.add(randomFileName);
      return newDataOutputFile(
          String.format("%s/%s/%s", dataPath, partitionSpec.partitionToPath(partitionData), randomFileName));
    }

    @Override
    public OutputFile newDataOutputFile(String fileName) {
      // Slightly different naming compared to metadata files.
      UUID randomFileName = UUID.randomUUID();
      GENERATED_FILE_NAMES.add(randomFileName);
      return newOutputFile(dataFileNamesToPaths.computeIfAbsent(
          fileName,
          name -> String.format("%s/%s", dataPath, randomFileName)));
    }
  }
}
