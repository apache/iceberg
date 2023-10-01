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

import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.RowLevelOperationMode.MERGE_ON_READ;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DELETE_AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.DELETE_DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DELETE_MODE;
import static org.apache.iceberg.TableProperties.DELETE_ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.DELETE_PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.spark.SparkSQLProperties.COMPRESSION_CODEC;
import static org.apache.iceberg.spark.SparkSQLProperties.COMPRESSION_LEVEL;
import static org.apache.iceberg.spark.SparkSQLProperties.COMPRESSION_STRATEGY;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.SizeBasedFileRewriter;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestCompressionSettings extends SparkCatalogTestBase {

  private static final Configuration CONF = new Configuration();
  private static final String tableName = "testWriteData";

  private static SparkSession spark = null;

  private final FileFormat format;
  private final ImmutableMap<String, String> properties;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Parameterized.Parameters(name = "format = {0}, properties = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {"parquet", ImmutableMap.of(COMPRESSION_CODEC, "zstd", COMPRESSION_LEVEL, "1")},
      {"parquet", ImmutableMap.of(COMPRESSION_CODEC, "gzip")},
      {"orc", ImmutableMap.of(COMPRESSION_CODEC, "zstd", COMPRESSION_STRATEGY, "speed")},
      {"orc", ImmutableMap.of(COMPRESSION_CODEC, "zstd", COMPRESSION_STRATEGY, "compression")},
      {"avro", ImmutableMap.of(COMPRESSION_CODEC, "snappy", COMPRESSION_LEVEL, "3")}
    };
  }

  @BeforeClass
  public static void startSpark() {
    TestCompressionSettings.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @Parameterized.AfterParam
  public static void clearSourceCache() {
    spark.sql(String.format("DROP TABLE IF EXISTS %s", tableName));
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestCompressionSettings.spark;
    TestCompressionSettings.spark = null;
    currentSpark.stop();
  }

  public TestCompressionSettings(String format, ImmutableMap properties) {
    super(
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        SparkCatalogConfig.SPARK.properties());
    this.format = FileFormat.fromString(format);
    this.properties = properties;
  }

  @Test
  public void testWriteDataWithDifferentSetting() throws Exception {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(PARQUET_COMPRESSION, "gzip");
    tableProperties.put(AVRO_COMPRESSION, "gzip");
    tableProperties.put(ORC_COMPRESSION, "zlib");
    tableProperties.put(DELETE_PARQUET_COMPRESSION, "gzip");
    tableProperties.put(DELETE_AVRO_COMPRESSION, "gzip");
    tableProperties.put(DELETE_ORC_COMPRESSION, "zlib");
    tableProperties.put(DELETE_MODE, MERGE_ON_READ.modeName());
    tableProperties.put(FORMAT_VERSION, "2");
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s')", tableName, DEFAULT_FILE_FORMAT, format);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s')",
        tableName, DELETE_DEFAULT_FILE_FORMAT, format);
    for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s')",
          tableName, entry.getKey(), entry.getValue());
    }

    List<SimpleRecord> expectedOrigin = Lists.newArrayList();
    for (int i = 0; i < 1000; i++) {
      expectedOrigin.add(new SimpleRecord(i, "hello world" + i));
    }

    Dataset<Row> df = spark.createDataFrame(expectedOrigin, SimpleRecord.class);

    for (Map.Entry<String, String> entry : properties.entrySet()) {
      spark.conf().set(entry.getKey(), entry.getValue());
    }

    df.select("id", "data")
        .writeTo(tableName)
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .append();
    Table table = catalog.loadTable(TableIdentifier.of("default", tableName));
    List<ManifestFile> manifestFiles = table.currentSnapshot().dataManifests(table.io());
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifestFiles.get(0), table.io())) {
      DataFile file = reader.iterator().next();
      InputFile inputFile = table.io().newInputFile(file.path().toString());
      Assertions.assertThat(getCompressionType(inputFile))
          .isEqualToIgnoringCase(properties.get(COMPRESSION_CODEC));
    }

    sql("DELETE from %s where id < 100", tableName);

    table.refresh();
    List<ManifestFile> deleteManifestFiles = table.currentSnapshot().deleteManifests(table.io());
    Map<Integer, PartitionSpec> specMap = Maps.newHashMap();
    specMap.put(0, PartitionSpec.unpartitioned());
    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(deleteManifestFiles.get(0), table.io(), specMap)) {
      DeleteFile file = reader.iterator().next();
      InputFile inputFile = table.io().newInputFile(file.path().toString());
      Assertions.assertThat(getCompressionType(inputFile))
          .isEqualToIgnoringCase(properties.get(COMPRESSION_CODEC));
    }

    SparkActions.get(spark)
        .rewritePositionDeletes(table)
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .execute();
    table.refresh();
    deleteManifestFiles = table.currentSnapshot().deleteManifests(table.io());
    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(deleteManifestFiles.get(0), table.io(), specMap)) {
      DeleteFile file = reader.iterator().next();
      InputFile inputFile = table.io().newInputFile(file.path().toString());
      Assertions.assertThat(getCompressionType(inputFile))
          .isEqualToIgnoringCase(properties.get(COMPRESSION_CODEC));
    }
  }

  private String getCompressionType(InputFile inputFile) throws Exception {
    switch (format) {
      case ORC:
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(CONF).useUTCTimestamp(true);
        Reader orcReader = OrcFile.createReader(new Path(inputFile.location()), readerOptions);
        return orcReader.getCompressionKind().name();
      case PARQUET:
        ParquetMetadata footer =
            ParquetFileReader.readFooter(CONF, new Path(inputFile.location()), NO_FILTER);
        return footer.getBlocks().get(0).getColumns().get(0).getCodec().name();
      default:
        FileContext fc = FileContext.getFileContext(CONF);
        GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
        DataFileReader fileReader =
            (DataFileReader)
                DataFileReader.openReader(
                    new AvroFSInput(fc, new Path(inputFile.location())), reader);
        return fileReader.getMetaString(DataFileConstants.CODEC);
    }
  }
}
