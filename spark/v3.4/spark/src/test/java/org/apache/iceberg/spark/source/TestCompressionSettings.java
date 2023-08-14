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

import static org.apache.iceberg.FileFormat.AVRO;
import static org.apache.iceberg.FileFormat.ORC;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.spark.SparkSQLProperties.COMPRESSION_CODEC;
import static org.apache.iceberg.spark.SparkSQLProperties.COMPRESSION_LEVEL;
import static org.apache.iceberg.spark.SparkSQLProperties.COMPRESSION_STRATEGY;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestCompressionSettings {

  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  private static SparkSession spark = null;

  @TempDir private File temp;

  private static Stream<Arguments> parameters() {
    return Stream.of(
        Arguments.of(PARQUET, ImmutableMap.of(COMPRESSION_CODEC, "zstd", COMPRESSION_LEVEL, "1")),
        Arguments.of(PARQUET, ImmutableMap.of(COMPRESSION_CODEC, "gzip")),
        Arguments.of(
            ORC, ImmutableMap.of(COMPRESSION_CODEC, "zstd", COMPRESSION_STRATEGY, "speed")),
        Arguments.of(
            ORC, ImmutableMap.of(COMPRESSION_CODEC, "zstd", COMPRESSION_STRATEGY, "compression")),
        Arguments.of(AVRO, ImmutableMap.of(COMPRESSION_CODEC, "snappy", COMPRESSION_LEVEL, "3")));
  }

  @BeforeAll
  public static void startSpark() {
    TestCompressionSettings.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterEach
  public void clearSourceCache() {
    ManualSource.clearTables();
  }

  @AfterAll
  public static void stopSpark() {
    SparkSession currentSpark = TestCompressionSettings.spark;
    TestCompressionSettings.spark = null;
    currentSpark.stop();
  }

  @ParameterizedTest(name = "format = {0}, properties = {1}")
  @MethodSource("parameters")
  public void testWriteDataWithDifferentSetting(
      FileFormat format, ImmutableMap<String, String> properties) throws Exception {
    File parent = temp;
    File location = new File(parent, "test");
    HadoopTables tables = new HadoopTables(CONF);
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(PARQUET_COMPRESSION, "gzip");
    tableProperties.put(AVRO_COMPRESSION, "gzip");
    tableProperties.put(ORC_COMPRESSION, "zlib");
    Table table =
        tables.create(SCHEMA, PartitionSpec.unpartitioned(), tableProperties, location.toString());
    List<SimpleRecord> expectedOrigin = Lists.newArrayList();
    for (int i = 0; i < 1000; i++) {
      expectedOrigin.add(new SimpleRecord(1, "hello world" + i));
    }

    Dataset<Row> df = spark.createDataFrame(expectedOrigin, SimpleRecord.class);

    for (Map.Entry<String, String> entry : properties.entrySet()) {
      spark.conf().set(entry.getKey(), entry.getValue());
    }

    df.select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    List<ManifestFile> manifestFiles = table.currentSnapshot().dataManifests(table.io());
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifestFiles.get(0), table.io())) {
      DataFile file = reader.iterator().next();
      InputFile inputFile = table.io().newInputFile(file.path().toString());
      Assertions.assertThat(getCompressionType(format, inputFile))
          .isEqualToIgnoringCase(properties.get(COMPRESSION_CODEC));
    }
  }

  private String getCompressionType(FileFormat format, InputFile inputFile) throws Exception {
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
