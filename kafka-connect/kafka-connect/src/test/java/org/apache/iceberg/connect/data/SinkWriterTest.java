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
package org.apache.iceberg.connect.data;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.routers.AllTableRecordRouter;
import org.apache.iceberg.connect.data.routers.DynamicRecordRouter;
import org.apache.iceberg.connect.data.routers.HeaderRecordRouter;
import org.apache.iceberg.connect.data.routers.RouterConstants;
import org.apache.iceberg.connect.data.routers.StaticRecordRouter;
import org.apache.iceberg.connect.data.routers.TopicRecordRouter;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SinkWriterTest {

  private InMemoryCatalog catalog;

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, TABLE_NAME);
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(3, "date", Types.StringType.get()));
  private static final String ROUTE_FIELD = "fld";

  @BeforeEach
  public void before() {
    catalog = initInMemoryCatalog();
    catalog.createNamespace(NAMESPACE);
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  private InMemoryCatalog initInMemoryCatalog() {
    InMemoryCatalog inMemoryCatalog = new InMemoryCatalog();
    inMemoryCatalog.initialize(null, ImmutableMap.of());
    return inMemoryCatalog;
  }

  @Test
  public void testDefaultRoute() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    AllTableRecordRouter allTableRecordRouter = new AllTableRecordRouter();
    allTableRecordRouter.configure(ImmutableMap.of("iceberg.tables", TABLE_IDENTIFIER.toString()));

    when(config.recordRouter()).thenReturn(allTableRecordRouter);
    Map<String, Object> value = ImmutableMap.of();

    List<IcebergWriterResult> writerResults = sinkWriterTest(value, config);
    assertThat(writerResults.size()).isEqualTo(1);
    IcebergWriterResult writerResult = writerResults.get(0);
    assertThat(writerResult.tableIdentifier()).isEqualTo(TABLE_IDENTIFIER);
  }

  @Test
  public void testDefaultNoRoute() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    AllTableRecordRouter allTableRecordRouter = new AllTableRecordRouter();
    allTableRecordRouter.configure(ImmutableMap.of());
    when(config.recordRouter()).thenReturn(allTableRecordRouter);
    Map<String, Object> value = ImmutableMap.of();

    List<IcebergWriterResult> writerResults = sinkWriterTest(value, config);
    assertThat(writerResults.size()).isEqualTo(0);
  }

  @Test
  public void testStaticRoute() {
    TableSinkConfig tableConfig = mock(TableSinkConfig.class);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(tableConfig);

    StaticRecordRouter staticRecordRouter = new StaticRecordRouter();
    staticRecordRouter.configure(
        ImmutableMap.of(
            "iceberg.tables.route-field",
            ROUTE_FIELD,
            "iceberg.tables",
            TABLE_IDENTIFIER.toString(),
            "iceberg.table." + "tbl" + ".route-regex",
            "val"));
    when(config.recordRouter()).thenReturn(staticRecordRouter);

    Map<String, Object> value = ImmutableMap.of(ROUTE_FIELD, "val");
    List<IcebergWriterResult> writerResults = sinkWriterTest(value, config);
    assertThat(writerResults.size()).isEqualTo(1);
    IcebergWriterResult writerResult = writerResults.get(0);
    assertThat(writerResult.tableIdentifier()).isEqualTo(TABLE_IDENTIFIER);
  }

  @Test
  public void testStaticNoRoute() {
    TableSinkConfig tableConfig = mock(TableSinkConfig.class);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(tableConfig);

    StaticRecordRouter staticRecordRouter = new StaticRecordRouter();
    staticRecordRouter.configure(
        ImmutableMap.of(
            "iceberg.tables.route-field",
            ROUTE_FIELD,
            "iceberg.tables",
            TABLE_IDENTIFIER.toString()));
    when(config.recordRouter()).thenReturn(staticRecordRouter);

    Map<String, Object> value = ImmutableMap.of(ROUTE_FIELD, "foobar");
    List<IcebergWriterResult> writerResults = sinkWriterTest(value, config);
    assertThat(writerResults.size()).isEqualTo(0);
  }

  @Test
  public void testDynamicRoute() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));

    DynamicRecordRouter dynamicRecordRouter = new DynamicRecordRouter();
    dynamicRecordRouter.configure(ImmutableMap.of("iceberg.tables.route-field", ROUTE_FIELD));
    when(config.recordRouter()).thenReturn(dynamicRecordRouter);

    Map<String, Object> value = ImmutableMap.of(ROUTE_FIELD, TABLE_IDENTIFIER.toString());

    List<IcebergWriterResult> writerResults = sinkWriterTest(value, config);
    assertThat(writerResults.size()).isEqualTo(1);
    IcebergWriterResult writerResult = writerResults.get(0);
    assertThat(writerResult.tableIdentifier()).isEqualTo(TABLE_IDENTIFIER);
  }

  @Test
  public void testDynamicNoRoute() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));

    DynamicRecordRouter dynamicRecordRouter = new DynamicRecordRouter();
    dynamicRecordRouter.configure(ImmutableMap.of("iceberg.tables.route-field", ROUTE_FIELD));
    when(config.recordRouter()).thenReturn(dynamicRecordRouter);

    Map<String, Object> value = ImmutableMap.of(ROUTE_FIELD, "db.foobar");

    List<IcebergWriterResult> writerResults = sinkWriterTest(value, config);
    assertThat(writerResults.size()).isEqualTo(0);
  }

  @Test
  public void testHeaderRouteOnHeaderValue() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));

    HeaderRecordRouter headerRecordRouter = new HeaderRecordRouter();
    headerRecordRouter.configure(ImmutableMap.of(RouterConstants.HEADER_ROUTE_KEYS, "tableName"));
    when(config.recordRouter()).thenReturn(headerRecordRouter);

    Map<String, Object> value = ImmutableMap.of(ROUTE_FIELD, TABLE_IDENTIFIER.toString());

    List<IcebergWriterResult> writerResults = sinkWriterTest(value, config);
    assertThat(writerResults.size()).isEqualTo(1);
    IcebergWriterResult writerResult = writerResults.get(0);
    assertThat(writerResult.tableIdentifier()).isEqualTo(TABLE_IDENTIFIER);
  }

  @Test
  public void testHeaderRouteOnTableMapping() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));

    HeaderRecordRouter headerRecordRouter = new HeaderRecordRouter();
    headerRecordRouter.configure(
        ImmutableMap.of(
            RouterConstants.HEADER_ROUTE_KEYS,
            "MappedTable",
            RouterConstants.TABLE_MAPPING,
            "tableNameFromMap:" + TABLE_IDENTIFIER));
    when(config.recordRouter()).thenReturn(headerRecordRouter);

    Map<String, Object> value = ImmutableMap.of(ROUTE_FIELD, TABLE_IDENTIFIER.toString());

    List<IcebergWriterResult> writerResults = sinkWriterTest(value, config);
    assertThat(writerResults.size()).isEqualTo(1);
    IcebergWriterResult writerResult = writerResults.get(0);
    assertThat(writerResult.tableIdentifier()).isEqualTo(TABLE_IDENTIFIER);
  }

  @Test
  public void testTopicRoute() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));

    TopicRecordRouter topicRecordRouter = new TopicRecordRouter();
    topicRecordRouter.configure(
        ImmutableMap.of(RouterConstants.TABLE_MAPPING, "topic:" + TABLE_IDENTIFIER));
    when(config.recordRouter()).thenReturn(topicRecordRouter);

    Map<String, Object> value = ImmutableMap.of(ROUTE_FIELD, TABLE_IDENTIFIER.toString());

    List<IcebergWriterResult> writerResults = sinkWriterTest(value, config);
    assertThat(writerResults.size()).isEqualTo(1);
    IcebergWriterResult writerResult = writerResults.get(0);
    assertThat(writerResult.tableIdentifier()).isEqualTo(TABLE_IDENTIFIER);
  }

  private List<IcebergWriterResult> sinkWriterTest(
      Map<String, Object> value, IcebergSinkConfig config) {
    IcebergWriterResult writeResult =
        new IcebergWriterResult(
            TableIdentifier.parse(TABLE_NAME),
            ImmutableList.of(mock(DataFile.class)),
            ImmutableList.of(),
            Types.StructType.of());
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete()).thenReturn(ImmutableList.of(writeResult));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    SinkWriter sinkWriter = new SinkWriter(catalog, config);

    // save a record
    Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    SinkRecord rec =
        new SinkRecord(
            "topic",
            1,
            null,
            "key",
            null,
            value,
            100L,
            now.toEpochMilli(),
            TimestampType.LOG_APPEND_TIME);
    rec.headers().addBytes("MappedTable", "tableNameFromMap".getBytes(StandardCharsets.UTF_8));
    rec.headers()
        .addBytes("tableName", TABLE_IDENTIFIER.toString().getBytes(StandardCharsets.UTF_8));
    sinkWriter.save(ImmutableList.of(rec));

    SinkWriterResult result = sinkWriter.completeWrite();

    Offset offset = result.sourceOffsets().get(new TopicPartition("topic", 1));
    assertThat(offset).isNotNull();
    assertThat(offset.offset()).isEqualTo(101L); // should be 1 more than current offset
    assertThat(offset.timestamp()).isEqualTo(now.atOffset(ZoneOffset.UTC));

    return result.writerResults();
  }
}
