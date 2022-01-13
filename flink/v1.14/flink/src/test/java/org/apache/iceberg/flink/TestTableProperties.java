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

package org.apache.iceberg.flink;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.file.CodecFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
public class TestTableProperties extends FlinkCatalogTestBase {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private final boolean isStreamingJob;
  private final FileFormat format;

  private TableEnvironment tEnv;

  public TestTableProperties(String catalogName, Namespace baseNamespace, FileFormat format, Boolean isStreamingJob) {
    super(catalogName, baseNamespace);
    this.format = format;
    this.isStreamingJob = isStreamingJob;
  }

  @Parameterized.Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}, isStreaming={3}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format : new FileFormat[] {FileFormat.ORC, FileFormat.AVRO, FileFormat.PARQUET}) {
      for (Boolean isStreaming : new Boolean[] {true, false}) {
        for (Object[] catalogParams : FlinkCatalogTestBase.parameters()) {
          String catalogName = (String) catalogParams[0];
          Namespace baseNamespace = (Namespace) catalogParams[1];
          parameters.add(new Object[] {catalogName, baseNamespace, format, isStreaming});
        }
      }
    }
    return parameters;
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings
            .newInstance();
        if (isStreamingJob) {
          settingsBuilder.inStreamingMode();
          StreamExecutionEnvironment env = StreamExecutionEnvironment
              .getExecutionEnvironment(MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG);
          env.enableCheckpointing(400);
          env.setMaxParallelism(2);
          env.setParallelism(2);
          tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
        } else {
          settingsBuilder.inBatchMode();
          tEnv = TableEnvironment.create(settingsBuilder.build());
        }
      }
    }
    return tEnv;
  }

  @Override
  @Before
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
  }

  @Override
  @After
  public void clean() {
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @Test
  public void testParquetTableProperties() throws Exception {
    if (format != FileFormat.PARQUET) {
      return;
    }
    String parquetTableName = "test_table_parquet";

    String groupSizeBytes = "10000";
    String pageSizeBytes = "10000";
    String dictSizeBytes = "10000";
    String compressionCodec = "uncompressed";

    sql("CREATE TABLE %s (id int, data varchar) with (" +
            "'" + TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES + "'='" + groupSizeBytes + "'," +
            "'" + TableProperties.PARQUET_PAGE_SIZE_BYTES + "'='" + pageSizeBytes + "'," +
            "'" + TableProperties.PARQUET_DICT_SIZE_BYTES + "'='" + dictSizeBytes + "'," +
            "'" + TableProperties.PARQUET_COMPRESSION + "'='" + compressionCodec + "'," +
            "'" + TableProperties.DEFAULT_FILE_FORMAT + "'='%s')",
        parquetTableName, format);
    Table icebergTable = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, parquetTableName));

    Parquet.WriteBuilder writeBuilder = Mockito.spy(Parquet.write(Files.localOutput(TEMPORARY_FOLDER.newFile())));

    writeBuilder.forTable(icebergTable);
    ArgumentCaptor<Map<String, String>> argument = ArgumentCaptor.forClass(Map.class);
    verify(writeBuilder).setAll(argument.capture());
    Map<String, String> config = argument.getValue();

    Assert.assertEquals(groupSizeBytes, config.get(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES));
    Assert.assertEquals(pageSizeBytes, config.get(TableProperties.PARQUET_PAGE_SIZE_BYTES));
    Assert.assertEquals(dictSizeBytes, config.get(TableProperties.PARQUET_DICT_SIZE_BYTES));
    Assert.assertEquals(compressionCodec, config.get(TableProperties.PARQUET_COMPRESSION));
    Assert.assertEquals(FileFormat.PARQUET.name(), config.get(TableProperties.DEFAULT_FILE_FORMAT));

    DynFields.BoundField<Function> createContextFunc =
        DynFields.builder().hiddenImpl(Parquet.WriteBuilder.class, "createContextFunc").build(writeBuilder);
    Object apply = createContextFunc.get().apply(config);

    Integer rowGroupSize =
        (Integer) DynFields.builder().hiddenImpl(apply.getClass(), "rowGroupSize").build(apply).get();
    Assert.assertEquals(groupSizeBytes, String.valueOf(rowGroupSize));

    Integer pageSize =
        (Integer) DynFields.builder().hiddenImpl(apply.getClass(), "pageSize").build(apply).get();
    Assert.assertEquals(pageSizeBytes, String.valueOf(pageSize));

    Integer dictionaryPageSize =
        (Integer) DynFields.builder().hiddenImpl(apply.getClass(), "dictionaryPageSize").build(apply).get();
    Assert.assertEquals(dictSizeBytes, String.valueOf(dictionaryPageSize));

    CompressionCodecName codec =
        (CompressionCodecName) DynFields.builder().hiddenImpl(apply.getClass(), "codec").build(apply).get();
    Assert.assertEquals(CompressionCodecName.UNCOMPRESSED, codec);

    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, parquetTableName);
  }

  @Test
  public void testAvroTableProperties() throws Exception {
    if (format != FileFormat.AVRO) {
      return;
    }
    String avroTableName = "test_table_avro";
    String compressionCodec = "snappy";

    sql("CREATE TABLE %s (id int, data varchar) with (" +
            "'" + TableProperties.AVRO_COMPRESSION + "'='" + compressionCodec + "'," +
            "'" + TableProperties.DEFAULT_FILE_FORMAT + "'='%s')",
        avroTableName, format);
    Table icebergTable = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, avroTableName));

    Avro.WriteBuilder writeBuilder = Mockito.spy(Avro.write(Files.localOutput(TEMPORARY_FOLDER.newFile())));

    writeBuilder.forTable(icebergTable);
    ArgumentCaptor<Map<String, String>> argument = ArgumentCaptor.forClass(Map.class);
    verify(writeBuilder).setAll(argument.capture());
    Map<String, String> config = argument.getValue();

    Assert.assertEquals(compressionCodec, config.get(TableProperties.AVRO_COMPRESSION));
    Assert.assertEquals(FileFormat.AVRO.name(), config.get(TableProperties.DEFAULT_FILE_FORMAT));

    DynFields.BoundField<Function> createContextFunc =
        DynFields.builder().hiddenImpl(Avro.WriteBuilder.class, "createContextFunc").build(writeBuilder);
    Object apply = createContextFunc.get().apply(config);

    CodecFactory codecFactory =
        (CodecFactory) DynFields.builder().hiddenImpl(apply.getClass(), "codec").build(apply).get();

    Assert.assertEquals(CodecFactory.snappyCodec().toString(), codecFactory.toString());
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, avroTableName);
  }

  @Ignore // TODO: test orc table properties
  public void testORCTableProperties() throws Exception {
    if (format != FileFormat.ORC) {
      return;
    }
  }
}
