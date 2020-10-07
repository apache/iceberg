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

package org.apache.iceberg.aws.glue.convertors;

import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Order;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.SkewedInfo;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.iceberg.aws.glue.converters.CatalogToHiveConverter;
import org.apache.iceberg.aws.glue.converters.HiveToCatalogConverter;
import org.apache.iceberg.aws.glue.util.ObjectTestUtils;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestEntityConversion {

  private static final String TEST_DB_NAME = "testDb";
  private static final String TEST_TBL_NAME = "testTbl";

  @Test
  public void testDatabaseConversion() {
    Database catalogDb = ObjectTestUtils.getTestDatabase();
    org.apache.hadoop.hive.metastore.api.Database hiveDatabase = CatalogToHiveConverter
        .convertDatabase(catalogDb);
    Database catalogDb2 = HiveToCatalogConverter.convertDatabase(hiveDatabase);
    assertEquals(catalogDb, catalogDb2);
  }

  @Test
  public void testDatabaseConversionWithNullFields() {
    Database catalogDb = ObjectTestUtils.getTestDatabase();
    catalogDb.setLocationUri(null);
    catalogDb.setParameters(null);
    org.apache.hadoop.hive.metastore.api.Database hiveDatabase = CatalogToHiveConverter
        .convertDatabase(catalogDb);
    assertThat(hiveDatabase.getLocationUri(), is(""));
    assertNotNull(hiveDatabase.getParameters());
  }

  @Test
  public void testExceptionTranslation() {
    assertEquals("org.apache.hadoop.hive.metastore.api.AlreadyExistsException",
        CatalogToHiveConverter.wrapInHiveException(new AlreadyExistsException("")).getClass().getName());
  }

  @Test
  public void testTableConversion() {
    Table catalogTable = ObjectTestUtils.getTestTable();
    org.apache.hadoop.hive.metastore.api.Table hiveTable =
        CatalogToHiveConverter.convertTable(catalogTable, TEST_DB_NAME);
    assertEquals(catalogTable, HiveToCatalogConverter.convertTable(hiveTable));
  }

  @Test
  public void testTableConversionWithNullParameterMap() {
    // Test to ensure the parameter map returned to Hive is never null.
    Table catalogTable = ObjectTestUtils.getTestTable();
    catalogTable.setParameters(null);
    org.apache.hadoop.hive.metastore.api.Table hiveTable =
        CatalogToHiveConverter.convertTable(catalogTable, TEST_DB_NAME);
    assertNotNull(hiveTable.getParameters());
    assertTrue(hiveTable.getParameters().isEmpty());
  }

  @Test
  public void testPartitionConversion() {
    Partition partition = ObjectTestUtils.getTestPartition(TEST_DB_NAME, TEST_TBL_NAME, ImmutableList.of("1"));
    org.apache.hadoop.hive.metastore.api.Partition hivePartition = CatalogToHiveConverter.convertPartition(partition);
    Partition converted = HiveToCatalogConverter.convertPartition(hivePartition);
    assertEquals(partition, converted);
  }

  @Test
  public void testPartitionConversionWithNullParameterMap() {
    // Test to ensure the parameter map returned to Hive is never null.
    Partition partition = ObjectTestUtils.getTestPartition(TEST_DB_NAME, TEST_TBL_NAME, ImmutableList.of("1"));
    partition.setParameters(null);
    org.apache.hadoop.hive.metastore.api.Partition hivePartition = CatalogToHiveConverter.convertPartition(partition);
    assertNotNull(hivePartition.getParameters());
    assertTrue(hivePartition.getParameters().isEmpty());
  }

  @Test
  public void testConvertPartitions() {
    Partition partition = ObjectTestUtils.getTestPartition(
        TEST_DB_NAME, TEST_TBL_NAME, ImmutableList.of("value1", "value2"));
    org.apache.hadoop.hive.metastore.api.Partition hivePartition = CatalogToHiveConverter.convertPartition(partition);
    List<Partition> partitions = ImmutableList.of(partition);
    assertEquals(ImmutableList.of(hivePartition), CatalogToHiveConverter.convertPartitions(partitions));
  }

  @Test
  public void testConvertPartitionsEmpty() {
    assertEquals(ImmutableList.of(), CatalogToHiveConverter.convertPartitions(ImmutableList.<Partition>of()));
  }

  @Test
  public void testConvertPartitionsNull() {
    assertEquals(null, CatalogToHiveConverter.convertPartitions(null));
  }

  @Test
  public void testSkewedInfoConversion() {
    SkewedInfo catalogSkewedInfo = ObjectTestUtils.getSkewedInfo();
    org.apache.hadoop.hive.metastore.api.SkewedInfo hiveSkewedinfo =
        CatalogToHiveConverter.convertSkewedInfo(catalogSkewedInfo);
    assertEquals(catalogSkewedInfo, HiveToCatalogConverter.convertSkewedInfo(hiveSkewedinfo));
    assertEquals(null, HiveToCatalogConverter.convertSkewedInfo(null));
    assertEquals(null, CatalogToHiveConverter.convertSkewedInfo(null));
  }

  @Test
  public void testConvertSkewedInfoNullFields() {
    SkewedInfo catalogSkewedInfo = new SkewedInfo();
    org.apache.hadoop.hive.metastore.api.SkewedInfo hiveSkewedInfo =
        CatalogToHiveConverter.convertSkewedInfo(catalogSkewedInfo);
    assertNotNull(hiveSkewedInfo.getSkewedColNames());
    assertNotNull(hiveSkewedInfo.getSkewedColValues());
    assertNotNull(hiveSkewedInfo.getSkewedColValueLocationMaps());
  }

  @Test
  public void testConvertSerdeInfoNullParameter() {
    SerDeInfo serDeInfo = ObjectTestUtils.getTestSerdeInfo();
    serDeInfo.setParameters(null);
    assertNotNull(CatalogToHiveConverter.convertSerDeInfo(serDeInfo).getParameters());
  }

  @Test
  public void testFunctionConversion() {
    UserDefinedFunction catalogFunction = ObjectTestUtils.getCatalogTestFunction();
    org.apache.hadoop.hive.metastore.api.Function hiveFunction =
        CatalogToHiveConverter.convertFunction(TEST_DB_NAME, catalogFunction);
    assertEquals(TEST_DB_NAME, hiveFunction.getDbName());
    assertEquals(catalogFunction, HiveToCatalogConverter.convertFunction(hiveFunction));
  }

  @Test
  public void testConvertOrderList() {
    List<org.apache.hadoop.hive.metastore.api.Order> hiveOrderList = ImmutableList.of(ObjectTestUtils.getTestOrder());
    List<Order> catalogOrderList = HiveToCatalogConverter.convertOrderList(hiveOrderList);

    assertEquals(hiveOrderList.get(0).getCol(), catalogOrderList.get(0).getColumn());
    assertEquals(hiveOrderList.get(0).getOrder(), catalogOrderList.get(0).getSortOrder().intValue());
  }

  @Test
  public void testConvertOrderListNull() {
    assertNull(HiveToCatalogConverter.convertOrderList(null));
  }

  @Test
  public void testTableMetaConversion() {
    Table catalogTable = ObjectTestUtils.getTestTable();
    TableMeta tableMeta = CatalogToHiveConverter.convertTableMeta(catalogTable, TEST_DB_NAME);
    assertEquals(catalogTable.getName(), tableMeta.getTableName());
    assertEquals(TEST_DB_NAME, tableMeta.getDbName());
    assertEquals(catalogTable.getTableType(), tableMeta.getTableType());
  }

  @Test
  public void testTableConversionStorageDescriptorParameterMapNull() {
    Table catalogTable = ObjectTestUtils.getTestTable();
    catalogTable.getStorageDescriptor().setParameters(null);
    org.apache.hadoop.hive.metastore.api.Table hiveTable =
        CatalogToHiveConverter.convertTable(catalogTable, TEST_DB_NAME);
    assertNotNull(hiveTable.getSd().getParameters());
    assertTrue(hiveTable.getSd().getParameters().isEmpty());
  }

  @Test
  public void testTableConversionStorageDescriptorBucketColsNull() {
    Table catalogTable = ObjectTestUtils.getTestTable();
    catalogTable.getStorageDescriptor().setBucketColumns(null);
    org.apache.hadoop.hive.metastore.api.Table hiveTable =
        CatalogToHiveConverter.convertTable(catalogTable, TEST_DB_NAME);
    assertNotNull(hiveTable.getSd().getBucketCols());
    assertTrue(hiveTable.getSd().getBucketCols().isEmpty());
  }

  @Test
  public void testTableConversionStorageDescriptorSorColsNull() {
    Table catalogTable = ObjectTestUtils.getTestTable();
    catalogTable.getStorageDescriptor().setSortColumns(null);
    org.apache.hadoop.hive.metastore.api.Table hiveTable =
        CatalogToHiveConverter.convertTable(catalogTable, TEST_DB_NAME);
    assertNotNull(hiveTable.getSd().getSortCols());
    assertTrue(hiveTable.getSd().getSortCols().isEmpty());
  }

  @Test
  public void testTableConversionWithNullPartitionKeys() {
    Table catalogTable = ObjectTestUtils.getTestTable();
    catalogTable.setPartitionKeys(null);
    org.apache.hadoop.hive.metastore.api.Table hiveTable =
        CatalogToHiveConverter.convertTable(catalogTable, TEST_DB_NAME);
    assertNotNull(hiveTable.getPartitionKeys());
    assertTrue(hiveTable.getPartitionKeys().isEmpty());
  }
}
