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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.reader.ReaderUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantPrimitive;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

class TestFlinkVariantType extends CatalogTestBase {

  private static final String TABLE_NAME = "test_table";
  private Table icebergTable;
  @TempDir private Path warehouseDir;

  @Parameters(name = "catalogName={0}, baseNamespace={1}")
  protected static List<Object[]> parameters() {
    return Arrays.asList(
        // For now hive metadata is not supported variant, so we only test hadoop catalog
        new Object[] {"testhadoop", Namespace.empty()},
        new Object[] {"testhadoop_basenamespace", Namespace.of("l0", "l1")});
  }

  @Override
  @BeforeEach
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    sql(
        "CREATE TABLE %s (id int, data variant) with ('write.format.default'='%s','format-version'='3' )",
        TABLE_NAME, FileFormat.PARQUET.name());
    icebergTable = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
  }

  @TestTemplate
  public void testInsertVariantFromFlink() throws Exception {
    sql("INSERT INTO %s VALUES (1, PARSE_JSON('%s'))", TABLE_NAME, "{\"KeyA\":\"city\"}");
    List<Record> records = SimpleDataUtil.tableRecords(icebergTable);
    assertThat(records).hasSize(1);
    Object field = records.get(0).getField("data");
    assertThat(field).isInstanceOf(org.apache.iceberg.variants.Variant.class);
    org.apache.iceberg.variants.Variant variant = (org.apache.iceberg.variants.Variant) field;
    assertThat(variant.metadata().get(0)).isEqualTo("KeyA");
    assertThat(((VariantPrimitive<String>) variant.value().asObject().get("KeyA")).get())
        .isEqualTo("city");
  }

  @TestTemplate
  public void testReadVariantFromFlink() throws Exception {
    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    VariantMetadata metadata = Variants.metadata("name", "age");
    ShreddedObject shreddedObject = Variants.object(metadata);
    shreddedObject.put("name", Variants.of("John Doe"));
    shreddedObject.put("age", Variants.of((byte) 30));
    builder.add(
        GenericRecord.create(icebergTable.schema())
            .copy("id", 1, "data", Variant.of(metadata, shreddedObject)));
    new GenericAppenderHelper(icebergTable, FileFormat.PARQUET, warehouseDir)
        .appendToTable(builder.build());
    icebergTable.refresh();

    List<GenericRowData> genericRowData = Lists.newArrayList();
    CloseableIterable<CombinedScanTask> combinedScanTasks = icebergTable.newScan().planTasks();
    for (CombinedScanTask combinedScanTask : combinedScanTasks) {
      DataIterator<RowData> dataIterator =
          ReaderUtil.createDataIterator(
              combinedScanTask, icebergTable.schema(), icebergTable.schema());
      while (dataIterator.hasNext()) {
        GenericRowData rowData = (GenericRowData) dataIterator.next();
        genericRowData.add(rowData);
      }
    }

    assertThat(genericRowData).hasSize(1);
    assertThat(genericRowData.get(0).getField(1)).isInstanceOf(BinaryVariant.class);
    BinaryVariant binaryVariant = (BinaryVariant) genericRowData.get(0).getField(1);
    assertThat(binaryVariant.getField("name").getString()).isEqualTo("John Doe");
    assertThat(binaryVariant.getField("age").getByte()).isEqualTo((byte) 30);
  }
}
