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
package org.apache.iceberg.flink.source;

import static org.apache.iceberg.flink.TestHelpers.assertRecords;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;

/** Test Flink SQL Nest Projection. */
public class TestFlinkSqlNestProjection extends TestFlinkSource {

  private volatile TableEnvironment tEnv;

  public TestFlinkSqlNestProjection(String fileFormat) {
    super(fileFormat);
  }

  @Before
  public void before() throws IOException {
    SqlHelpers.sql(
        getTableEnv(),
        "create catalog iceberg_catalog with ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
        catalogResource.warehouse());
    SqlHelpers.sql(getTableEnv(), "use catalog iceberg_catalog");
    getTableEnv()
        .getConfig()
        .getConfiguration()
        .set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
  }

  private TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          this.tEnv =
              TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        }
      }
    }
    return tEnv;
  }

  @Override
  protected List<Row> run(
      FlinkSource.Builder formatBuilder,
      Map<String, String> sqlOptions,
      String sqlFilter,
      String... sqlSelectedFields) {
    String select = String.join(",", sqlSelectedFields);

    StringBuilder builder = new StringBuilder();
    sqlOptions.forEach((key, value) -> builder.append(optionToKv(key, value)).append(","));

    String optionStr = builder.toString();
    if (optionStr.endsWith(",")) {
      optionStr = optionStr.substring(0, optionStr.length() - 1);
    }
    if (!optionStr.isEmpty()) {
      optionStr = String.format("/*+ OPTIONS(%s)*/", optionStr);
    }

    return sql("select %s from t %s %s", select, optionStr, sqlFilter);
  }

  @Test
  public void testNestProjectionOneStruct() throws Exception {
    Types.StructType struct =
        Types.StructType.of(
            Types.NestedField.required(21, "a", Types.LongType.get()),
            Types.NestedField.optional(22, "b", Types.LongType.get()));

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "data", Types.StringType.get()),
            Types.NestedField.required(3, "id", Types.LongType.get()),
            Types.NestedField.required(4, "dt", Types.StringType.get()),
            Types.NestedField.required(5, "st", struct));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dt").bucket("id", 1).build();

    Table table =
        catalogResource.catalog().createTable(TableIdentifier.of("default", "t"), schema, spec);

    List<Record> writeRecords = RandomGenericData.generate(schema, 20, 0L);
    writeRecords.get(0).set(1, 123L);
    writeRecords.get(0).set(2, "2020-03-20");
    writeRecords.get(1).set(1, 456L);
    writeRecords.get(1).set(2, "2020-03-20");

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);
    DataFile dataFile = helper.writeFile(TestHelpers.Row.of("2020-03-20", 0), writeRecords);
    helper.appendToTable(dataFile);

    List<Row> result = sql("SELECT st.a, id, st.b FROM t");

    Schema schemaResult =
        new Schema(
            Types.NestedField.required(1, "st.a", Types.LongType.get()),
            Types.NestedField.required(2, "id", Types.LongType.get()),
            Types.NestedField.optional(3, "st.b", Types.LongType.get()));

    List<Record> expectedRecords = Lists.newArrayListWithExpectedSize(writeRecords.size());
    for (Record sourceRecord : writeRecords) {
      GenericRecord genericRecord = GenericRecord.create(schemaResult);
      genericRecord.set(1, sourceRecord.get(1));
      Object st = sourceRecord.get(3);
      if (st != null) {
        genericRecord.set(0, ((Record) st).get(0));
        genericRecord.set(2, ((Record) st).get(1));
      } else {
        genericRecord.set(0, null);
        genericRecord.set(2, null);
      }
      expectedRecords.add(genericRecord);
    }

    assertRecords(result, expectedRecords, schemaResult);
  }

  @Test
  public void testNestProjectionTwoStruct() throws Exception {
    Types.StructType struct =
        Types.StructType.of(
            Types.NestedField.required(21, "a", Types.LongType.get()),
            Types.NestedField.optional(22, "b", Types.LongType.get()));

    Types.StructType struct2 =
        Types.StructType.of(
            Types.NestedField.optional(31, "a", Types.StringType.get()),
            Types.NestedField.optional(32, "b", Types.StringType.get()));

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "data", Types.StringType.get()),
            Types.NestedField.required(3, "id", Types.LongType.get()),
            Types.NestedField.required(4, "dt", Types.StringType.get()),
            Types.NestedField.required(5, "st", struct),
            Types.NestedField.required(6, "st2", struct2));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dt").bucket("id", 1).build();

    Table table =
        catalogResource.catalog().createTable(TableIdentifier.of("default", "t"), schema, spec);

    List<Record> writeRecords = RandomGenericData.generate(schema, 20, 0L);
    writeRecords.get(0).set(1, 123L);
    writeRecords.get(0).set(2, "2020-03-20");
    writeRecords.get(1).set(1, 456L);
    writeRecords.get(1).set(2, "2020-03-20");

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);
    DataFile dataFile = helper.writeFile(TestHelpers.Row.of("2020-03-20", 0), writeRecords);
    helper.appendToTable(dataFile);

    List<Row> result = sql("SELECT id, st.b, st2.b FROM t");

    Schema schemaResult =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "st.b", Types.LongType.get()),
            Types.NestedField.optional(3, "st2.b", Types.StringType.get()));

    List<Record> expectedRecords = Lists.newArrayListWithExpectedSize(writeRecords.size());
    for (Record sourceRecord : writeRecords) {
      GenericRecord genericRecord = GenericRecord.create(schemaResult);
      genericRecord.set(0, sourceRecord.get(1));
      Record st = (Record) sourceRecord.get(3);
      if (st != null) {
        genericRecord.set(1, st.get(1));
      } else {
        genericRecord.set(1, null);
      }
      Record st2 = (Record) sourceRecord.get(4);
      if (st2 != null) {
        genericRecord.set(2, st2.get(1));
      } else {
        genericRecord.set(2, null);
      }
      expectedRecords.add(genericRecord);
    }

    assertRecords(result, expectedRecords, schemaResult);
  }

  @Test
  public void testNestProjectionMultilayerStruct() throws Exception {
    Types.StructType struct2 =
        Types.StructType.of(
            Types.NestedField.optional(41, "a", Types.StringType.get()),
            Types.NestedField.optional(42, "b", Types.StringType.get()));

    Types.StructType struct =
        Types.StructType.of(
            Types.NestedField.optional(51, "a", Types.StringType.get()),
            Types.NestedField.optional(52, "b", struct2));

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "data", Types.StringType.get()),
            Types.NestedField.required(3, "id", Types.LongType.get()),
            Types.NestedField.required(4, "dt", Types.StringType.get()),
            Types.NestedField.required(7, "st", struct));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dt").bucket("id", 1).build();

    Table table =
        catalogResource.catalog().createTable(TableIdentifier.of("default", "t"), schema, spec);

    List<Record> writeRecords = RandomGenericData.generate(schema, 20, 0L);
    writeRecords.get(0).set(1, 123L);
    writeRecords.get(0).set(2, "2020-03-20");
    writeRecords.get(1).set(1, 456L);
    writeRecords.get(1).set(2, "2020-03-20");

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);
    DataFile dataFile = helper.writeFile(TestHelpers.Row.of("2020-03-20", 0), writeRecords);
    helper.appendToTable(dataFile);

    List<Row> result = sql("SELECT id, st.b.a FROM t");

    Schema schemaResult =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "st.b.a", Types.StringType.get()));

    List<Record> expectedRecords = Lists.newArrayListWithExpectedSize(writeRecords.size());
    for (Record sourceRecord : writeRecords) {
      GenericRecord genericRecord = GenericRecord.create(schemaResult);
      genericRecord.set(0, sourceRecord.get(1));
      Object stb = ((Record) sourceRecord.get(3)).get(1);
      if (stb != null) {
        genericRecord.set(1, ((Record) stb).get(0));
      } else {
        genericRecord.set(1, null);
      }
      expectedRecords.add(genericRecord);
    }

    assertRecords(result, expectedRecords, schemaResult);
  }

  @Test
  public void testNestProjectionMap() throws Exception {
    Types.MapType map =
        Types.MapType.ofOptional(11, 12, Types.IntegerType.get(), Types.StringType.get());

    Types.StructType struct =
        Types.StructType.of(
            Types.NestedField.required(21, "a", Types.LongType.get()),
            Types.NestedField.optional(22, "b", Types.LongType.get()));

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "data", Types.StringType.get()),
            Types.NestedField.required(3, "id", Types.LongType.get()),
            Types.NestedField.required(4, "dt", Types.StringType.get()),
            Types.NestedField.optional(2, "mp", map),
            Types.NestedField.required(5, "st", struct));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dt").bucket("id", 1).build();

    Table table =
        catalogResource.catalog().createTable(TableIdentifier.of("default", "t"), schema, spec);

    List<Record> writeRecords = RandomGenericData.generate(schema, 20, 0L);
    writeRecords.get(0).set(1, 123L);
    writeRecords.get(0).set(2, "2020-03-20");
    writeRecords.get(1).set(1, 456L);
    writeRecords.get(1).set(2, "2020-03-20");

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);
    DataFile dataFile = helper.writeFile(TestHelpers.Row.of("2020-03-20", 0), writeRecords);
    helper.appendToTable(dataFile);

    List<Row> result = sql("SELECT st.a, mp FROM t");

    Schema schemaResult =
        new Schema(
            Types.NestedField.required(1, "st.a", Types.LongType.get()),
            Types.NestedField.optional(2, "mp", map));

    List<Record> expectedRecords = Lists.newArrayListWithExpectedSize(writeRecords.size());
    for (Record sourceRecord : writeRecords) {
      GenericRecord genericRecord = GenericRecord.create(schemaResult);
      genericRecord.set(0, ((Record) sourceRecord.get(4)).get(0));
      genericRecord.set(1, sourceRecord.get(3));
      expectedRecords.add(genericRecord);
    }

    assertRecords(result, expectedRecords, schemaResult);
  }

  private List<Row> sql(String query, Object... args) {
    TableResult tableResult = getTableEnv().executeSql(String.format(query, args));
    try (CloseableIterator<Row> iter = tableResult.collect()) {
      return Lists.newArrayList(iter);
    } catch (Exception e) {
      throw new RuntimeException("Failed to collect table result", e);
    }
  }

  private String optionToKv(String key, Object value) {
    return "'" + key + "'='" + value + "'";
  }
}
