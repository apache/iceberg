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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestUUIDArrayStructInternalRow extends TestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(
              2, "uuid_array", Types.ListType.ofOptional(3, Types.UUIDType.get())),
          Types.NestedField.optional(4, "data", Types.StringType.get()));

  private String tableLocation = null;

  @TempDir private Path temp;
  @TempDir private File tableDir;

  @BeforeEach
  public void setupTableLocation() throws Exception {
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testUUIDArrayConversion() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    var options = Maps.<String, String>newHashMap();
    options.put(TableProperties.FORMAT_VERSION, "2");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    // Create test data with UUID arrays
    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();
    UUID uuid3 = UUID.randomUUID();

    Record record = GenericRecord.create(SCHEMA);
    record.set(0, 1);
    record.set(1, Arrays.asList(uuid1, uuid2, uuid3));
    record.set(2, "test_data");

    // Create StructInternalRow and test array conversion
    StructInternalRow structRow = new StructInternalRow(SCHEMA.asStruct());
    structRow.setStruct(record);

    ArrayData arrayData = structRow.getArray(1);

    assertThat(arrayData).isNotNull();
    assertThat(arrayData.numElements()).isEqualTo(3);

    UTF8String firstElement = arrayData.getUTF8String(0);
    UTF8String secondElement = arrayData.getUTF8String(1);
    UTF8String thirdElement = arrayData.getUTF8String(2);

    assertThat(firstElement.toString()).isEqualTo(uuid1.toString());
    assertThat(secondElement.toString()).isEqualTo(uuid2.toString());
    assertThat(thirdElement.toString()).isEqualTo(uuid3.toString());
  }

  @Test
  public void testUUIDArrayWithNulls() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    var options = Maps.<String, String>newHashMap();
    options.put(TableProperties.FORMAT_VERSION, "2");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();

    // Create a record with UUID array containing nulls
    Record record = GenericRecord.create(SCHEMA);
    record.set(0, 1);
    record.set(1, Arrays.asList(uuid1, null, uuid2));
    record.set(2, "test_data");

    StructInternalRow structRow = new StructInternalRow(SCHEMA.asStruct());
    structRow.setStruct(record);

    ArrayData arrayData = structRow.getArray(1);

    assertThat(arrayData).isNotNull();
    assertThat(arrayData.numElements()).isEqualTo(3);

    UTF8String firstElement = arrayData.getUTF8String(0);
    assertThat(arrayData.isNullAt(1)).isTrue();
    UTF8String thirdElement = arrayData.getUTF8String(2);

    assertThat(firstElement.toString()).isEqualTo(uuid1.toString());
    assertThat(thirdElement.toString()).isEqualTo(uuid2.toString());
  }

  @Test
  public void testUUIDArrayIntegrationWithSpark() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    var options = Maps.<String, String>newHashMap();
    options.put(TableProperties.FORMAT_VERSION, "2");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();

    Dataset<Row> df =
        spark.sql(
            String.format(
                "SELECT 1 as id, array(cast('%s' as string), cast('%s' as string)) as uuid_array, 'data1' as data",
                uuid1.toString(), uuid2.toString()));

    df.write().format("iceberg").mode("append").save(tableLocation);

    table.refresh();

    // Verify data can be read back
    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<Row> actualRecords = resultDF.collectAsList();

    assertThat(actualRecords).hasSize(1);
    Row row = actualRecords.get(0);
    assertThat(row.getInt(0)).isEqualTo(1);
    assertThat(row.getString(2)).isEqualTo("data1");

    // Verify UUID array
    List<String> uuidArray = row.getList(1);
    assertThat(uuidArray).hasSize(2);
    assertThat(uuidArray.get(0)).isEqualTo(uuid1.toString());
    assertThat(uuidArray.get(1)).isEqualTo(uuid2.toString());
  }
}
