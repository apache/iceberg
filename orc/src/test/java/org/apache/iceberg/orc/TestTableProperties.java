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
package org.apache.iceberg.orc;

import java.io.File;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile.CompressionStrategy;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestTableProperties {

  public static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Test
  public void testOrcTableProperties() throws Exception {
    Random random = new Random();
    int numOfCodecs = CompressionKind.values().length;
    int numOfStrategies = CompressionStrategy.values().length;

    long stripeSizeBytes = 32L * 1024 * 1024;
    long blockSizeBytes = 128L * 1024 * 1024;
    String codecAsString = CompressionKind.values()[random.nextInt(numOfCodecs)].name();
    String strategyAsString = CompressionStrategy.values()[random.nextInt(numOfStrategies)].name();

    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            TableProperties.ORC_STRIPE_SIZE_BYTES, String.valueOf(stripeSizeBytes),
            TableProperties.ORC_BLOCK_SIZE_BYTES, String.valueOf(blockSizeBytes),
            TableProperties.ORC_COMPRESSION, codecAsString,
            TableProperties.ORC_COMPRESSION_STRATEGY, strategyAsString,
            TableProperties.DEFAULT_FILE_FORMAT, FileFormat.ORC.name());

    File folder = TEMPORARY_FOLDER.newFolder();

    String warehouse = folder.getAbsolutePath();
    String tablePath = warehouse.concat("/test");
    Assert.assertTrue("Should create the table path correctly.", new File(tablePath).mkdir());

    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = new HadoopTables().create(SCHEMA, spec, properties, tablePath);

    File testFile = TEMPORARY_FOLDER.newFile();
    Assert.assertTrue(testFile.delete());

    FileAppender<Record> writer =
        ORC.write(Files.localOutput(testFile))
            .forTable(table)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .build();

    DynFields.BoundField<Configuration> confField =
        DynFields.builder().hiddenImpl(writer.getClass(), "conf").build(writer);

    Configuration configuration = confField.get();
    Assert.assertEquals(blockSizeBytes, OrcConf.BLOCK_SIZE.getLong(configuration));
    Assert.assertEquals(stripeSizeBytes, OrcConf.STRIPE_SIZE.getLong(configuration));
    Assert.assertEquals(codecAsString, OrcConf.COMPRESS.getString(configuration));
    Assert.assertEquals(strategyAsString, OrcConf.COMPRESSION_STRATEGY.getString(configuration));
    Assert.assertEquals(
        FileFormat.ORC.name(), configuration.get(TableProperties.DEFAULT_FILE_FORMAT));
  }

  @Test
  public void testOrcTableDeleteProperties() throws Exception {
    Random random = new Random();
    int numOfCodecs = CompressionKind.values().length;
    int numOfStrategies = CompressionStrategy.values().length;

    long stripeSizeBytes = 32L * 1024 * 1024;
    long blockSizeBytes = 128L * 1024 * 1024;
    String codecAsString = CompressionKind.values()[random.nextInt(numOfCodecs)].name();
    String strategyAsString = CompressionStrategy.values()[random.nextInt(numOfStrategies)].name();

    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            TableProperties.DELETE_ORC_STRIPE_SIZE_BYTES, String.valueOf(stripeSizeBytes),
            TableProperties.DELETE_ORC_BLOCK_SIZE_BYTES, String.valueOf(blockSizeBytes),
            TableProperties.DELETE_ORC_COMPRESSION, codecAsString,
            TableProperties.DELETE_ORC_COMPRESSION_STRATEGY, strategyAsString,
            TableProperties.DEFAULT_FILE_FORMAT, FileFormat.ORC.name());

    File folder = TEMPORARY_FOLDER.newFolder();

    String warehouse = folder.getAbsolutePath();
    String tablePath = warehouse.concat("/test");
    Assert.assertTrue("Should create the table path correctly.", new File(tablePath).mkdir());

    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = new HadoopTables().create(SCHEMA, spec, properties, tablePath);

    File testFile = TEMPORARY_FOLDER.newFile();
    Assert.assertTrue(testFile.delete());

    EqualityDeleteWriter<Object> deleteWriter =
        ORC.writeDeletes(Files.localOutput(testFile))
            .forTable(table)
            .equalityFieldIds(1)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .buildEqualityWriter();

    DynFields.BoundField<OrcFileAppender<Record>> writer =
        DynFields.builder().hiddenImpl(deleteWriter.getClass(), "appender").build(deleteWriter);

    OrcFileAppender<Record> orcFileAppender = writer.get();
    DynFields.BoundField<Configuration> confField =
        DynFields.builder().hiddenImpl(orcFileAppender.getClass(), "conf").build(orcFileAppender);

    Configuration configuration = confField.get();
    Assert.assertEquals(blockSizeBytes, OrcConf.BLOCK_SIZE.getLong(configuration));
    Assert.assertEquals(stripeSizeBytes, OrcConf.STRIPE_SIZE.getLong(configuration));
    Assert.assertEquals(codecAsString, OrcConf.COMPRESS.getString(configuration));
    Assert.assertEquals(strategyAsString, OrcConf.COMPRESSION_STRATEGY.getString(configuration));
    Assert.assertEquals(
        FileFormat.ORC.name(), configuration.get(TableProperties.DEFAULT_FILE_FORMAT));
  }
}
