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

package org.apache.iceberg.aws.glue.util;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.ErrorDetail;
import com.amazonaws.services.glue.model.Order;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import com.amazonaws.services.glue.model.PrincipalType;
import com.amazonaws.services.glue.model.ResourceType;
import com.amazonaws.services.glue.model.ResourceUri;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.SkewedInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.iceberg.aws.glue.converters.CatalogToHiveConverter;
import org.apache.iceberg.aws.glue.converters.HiveToCatalogConverter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import static org.apache.iceberg.aws.glue.converters.ConverterUtils.INDEX_DB_NAME;
import static org.apache.iceberg.aws.glue.converters.ConverterUtils.INDEX_DEFERRED_REBUILD;
import static org.apache.iceberg.aws.glue.converters.ConverterUtils.INDEX_HANDLER_CLASS;
import static org.apache.iceberg.aws.glue.converters.ConverterUtils.INDEX_ORIGIN_TABLE_NAME;
import static org.apache.iceberg.aws.glue.converters.ConverterUtils.INDEX_TABLE_NAME;

public final class ObjectTestUtils {

  private ObjectTestUtils() {
  }

  /**
   *
   * @return a test db
   */
  public static Database getTestDatabase() {

    Map<String, String> parameters = Maps.newHashMap();
    parameters.put("param1", "value1");
    parameters.put("param2", "value2");

    Database database = new Database()
        .withName("test-db-" + UUID.randomUUID().toString().replaceAll("[^a-zA-Z0-9]+", ""))
        .withDescription("database desc")
        .withLocationUri("/db")
        .withParameters(parameters);

    return database;
  }

  /**
   * @param len len
   * @return a random string of size len
   */
  public static String getStringOfLength(final int len) {
    StringBuffer sb = new StringBuffer(UUID.randomUUID().toString());
    for (int i = sb.length(); i < len; i++) {
      sb.append('0');
    }
    return sb.toString();
  }

  public static Table getTestTable() {
    Table table = new Table();
    table.setName("testtable" +
        UUID.randomUUID().toString().replaceAll("[^a-zA-Z0-9]+", "").substring(0, 4));
    table.setOwner("owner");
    table.setCreateTime(new Date(System.currentTimeMillis() / 1000 * 1000));
    table.setLastAccessTime(new Date(System.currentTimeMillis() / 1000 * 1000));
    table.setParameters(new HashMap<String, String>());
    table.setPartitionKeys(getTestFieldList());
    table.setStorageDescriptor(getTestStorageDescriptor());
    table.setTableType("MANAGED_TABLE");
    table.setRetention(1);
    table.setViewOriginalText("originalText");
    table.setViewExpandedText("expandedText");
    return table;
  }

  public static Table getTestTable(String dbName) {
    Table table = getTestTable();
    table.setDatabaseName(dbName);
    return table;
  }

  public static StorageDescriptor getTestStorageDescriptor() {
    StorageDescriptor sd = new StorageDescriptor();
    List<String> cols = new ArrayList<>();
    cols.add("sampleCols");
    sd.setBucketColumns(cols);
    sd.setColumns(getTestFieldList());
    sd.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(getTestSerdeInfo());
    sd.setSkewedInfo(getSkewedInfo());
    sd.setSortColumns(new ArrayList<Order>());
    sd.setInputFormat("inputFormat");
    sd.setOutputFormat("outputFormat");
    sd.setLocation("/test-table");
    sd.withSortColumns(new Order().withColumn("foo").withSortOrder(1));
    sd.setCompressed(false);
    sd.setStoredAsSubDirectories(false);
    sd.setNumberOfBuckets(0);
    return sd;
  }

  public static SerDeInfo getTestSerdeInfo() {
    return new SerDeInfo()
        .withName("serdeName")
        .withSerializationLibrary("serdeLib")
        .withParameters(new HashMap<String, String>());
  }

  public static List<Column> getTestFieldList() {
    List<Column> fieldList = new ArrayList<>();
    Column field = new Column()
        .withComment(UUID.randomUUID().toString())
        .withName("column" + UUID.randomUUID().toString().replaceAll("[^a-zA-Z0-9]+", ""))
        .withType("string");
    fieldList.add(field);
    return fieldList;
  }

  public static Index getTestHiveIndex(final String dbName) {
    Index index = new Index();
    index.setIndexName("testIndex" + UUID.randomUUID().toString().replaceAll("[^a-zA-Z0-9]+", ""));
    index.setCreateTime((int) (System.currentTimeMillis() / 1000));
    index.setLastAccessTime((int) (System.currentTimeMillis() / 1000));
    index.setDbName(dbName);
    index.setDeferredRebuild(false);
    index.setOrigTableName("OriginalTable");
    index.setIndexTableName("IndexTable");
    index.setIndexHandlerClass("handlerClass");
    index.setParameters(new HashMap<String, String>());
    index.setSd(CatalogToHiveConverter.convertStorageDescriptor(getTestStorageDescriptor()));

    return index;
  }

  public static void setIndexParametersForIndexTable(Table indexTable, String dbName, String originTableName) {
    indexTable.getParameters().put(INDEX_DEFERRED_REBUILD, "FALSE");
    indexTable.getParameters().put(INDEX_HANDLER_CLASS, "handlerClass");
    indexTable.getParameters().put(INDEX_DB_NAME, dbName);
    indexTable.getParameters().put(INDEX_ORIGIN_TABLE_NAME, originTableName);
    indexTable.getParameters().put(INDEX_TABLE_NAME, indexTable.getName());
  }

  public static SkewedInfo getSkewedInfo() {
    List<String> skewedName = new ArrayList<>();
    List<String> skewedValue = new ArrayList<>();
    List<String> skewedMapKey = new ArrayList<>();
    List<List<String>> skewedValueList = new ArrayList<>();
    skewedName.add(UUID.randomUUID().toString());
    skewedName.add(UUID.randomUUID().toString());
    skewedValue.add(UUID.randomUUID().toString());
    skewedValue.add(UUID.randomUUID().toString());
    skewedValueList.add(skewedValue);
    skewedMapKey.add(UUID.randomUUID().toString());
    skewedMapKey.add(UUID.randomUUID().toString());
    Map<String, String> skewedMap = new HashMap<>();
    skewedMap.put(HiveToCatalogConverter.convertListToString(skewedMapKey), UUID.randomUUID().toString());

    return new SkewedInfo().withSkewedColumnValueLocationMaps(skewedMap).withSkewedColumnNames(skewedName)
            .withSkewedColumnValues(HiveToCatalogConverter.convertSkewedValue(skewedValueList));
  }

  public static Partition  getTestPartition(String dbName, String tblName, List<String> values) {
    return new Partition()
        .withDatabaseName(dbName)
        .withTableName(tblName)
        .withValues(values)
        .withCreationTime(new Date(System.currentTimeMillis() / 1000 * 1000))
        .withLastAccessTime(new Date(System.currentTimeMillis() / 1000 * 1000))
        .withParameters(Maps.<String, String>newHashMap())
        .withStorageDescriptor(ObjectTestUtils.getTestStorageDescriptor());
  }

  public static UserDefinedFunction getCatalogTestFunction() {
    List<ResourceUri> resourceUriList = Lists.newArrayList(new ResourceUri().withUri("s3://abc/def.jar")
            .withResourceType(ResourceType.JAR), new ResourceUri().withUri("hdfs://ghi/jkl.jar")
            .withResourceType(ResourceType.ARCHIVE));
    return new UserDefinedFunction()
        .withFunctionName("functionname")
        .withClassName("classname")
        .withOwnerName("ownername")
        .withCreateTime(new Date(System.currentTimeMillis() / 1000 * 1000))
        .withOwnerType(PrincipalType.USER)
        .withResourceUris(resourceUriList);
  }


  private static ByteBuffer byteBuffer(long value) {
    return ByteBuffer.wrap(BigInteger.valueOf(value).toByteArray());
  }

  public static org.apache.hadoop.hive.metastore.api.ColumnStatisticsData getHiveBinaryColumnStatsData() {
    BinaryColumnStatsData statsData = new BinaryColumnStatsData();
    statsData.setAvgColLen(12.3);
    statsData.setMaxColLen(45L);
    statsData.setNumNulls(56L);
    org.apache.hadoop.hive.metastore.api.ColumnStatisticsData  statsWrapper =
            new org.apache.hadoop.hive.metastore.api.ColumnStatisticsData();
    statsWrapper.setBinaryStats(statsData);
    return statsWrapper;
  }

  public static org.apache.hadoop.hive.metastore.api.ColumnStatisticsData getHiveBooleanColumnStatsData() {
    BooleanColumnStatsData statsData = new BooleanColumnStatsData();
    statsData.setNumFalses(12L);
    statsData.setNumNulls(34L);
    statsData.setNumTrues(56L);
    org.apache.hadoop.hive.metastore.api.ColumnStatisticsData  statsWrapper =
            new org.apache.hadoop.hive.metastore.api.ColumnStatisticsData();
    statsWrapper.setBooleanStats(statsData);
    return statsWrapper;
  }

  public static org.apache.hadoop.hive.metastore.api.ColumnStatisticsData getHiveDecimalColumnStatsData() {
    DecimalColumnStatsData statsData = new DecimalColumnStatsData();
    org.apache.hadoop.hive.metastore.api.Decimal highValue = new org.apache.hadoop.hive.metastore.api.Decimal();
    highValue.setScale((short) 1);
    highValue.setUnscaled(BigInteger.valueOf(1234L).toByteArray());
    statsData.setHighValue(highValue);
    org.apache.hadoop.hive.metastore.api.Decimal lowValue = new org.apache.hadoop.hive.metastore.api.Decimal();
    lowValue.setScale((short) 4);
    lowValue.setUnscaled(BigInteger.valueOf(5678L).toByteArray());
    statsData.setLowValue(lowValue);
    statsData.setNumDVs(12L);
    statsData.setNumNulls(56L);
    org.apache.hadoop.hive.metastore.api.ColumnStatisticsData  statsWrapper =
            new org.apache.hadoop.hive.metastore.api.ColumnStatisticsData();
    statsWrapper.setDecimalStats(statsData);
    return statsWrapper;
  }

  public static org.apache.hadoop.hive.metastore.api.ColumnStatisticsData getHiveDoubleColumnStatsData() {
    DoubleColumnStatsData statsData = new DoubleColumnStatsData();
    statsData.setHighValue(9999.9);
    statsData.setLowValue(-1111.1);
    statsData.setNumDVs(123L);
    statsData.setNumNulls(456L);
    org.apache.hadoop.hive.metastore.api.ColumnStatisticsData  statsWrapper =
            new org.apache.hadoop.hive.metastore.api.ColumnStatisticsData();
    statsWrapper.setDoubleStats(statsData);
    return statsWrapper;
  }

  public static org.apache.hadoop.hive.metastore.api.ColumnStatisticsData getHiveLongColumnStatsData() {
    LongColumnStatsData statsData = new LongColumnStatsData();
    statsData.setHighValue(9999L);
    statsData.setLowValue(-1111L);
    statsData.setNumDVs(123L);
    statsData.setNumNulls(456L);
    org.apache.hadoop.hive.metastore.api.ColumnStatisticsData  statsWrapper =
            new org.apache.hadoop.hive.metastore.api.ColumnStatisticsData();
    statsWrapper.setLongStats(statsData);
    return statsWrapper;
  }

  public static org.apache.hadoop.hive.metastore.api.ColumnStatisticsData getHiveStringColumnStatsData() {
    StringColumnStatsData statsData = new StringColumnStatsData();
    statsData.setAvgColLen(123.4);
    statsData.setMaxColLen(567L);
    statsData.setNumDVs(89L);
    statsData.setNumNulls(13L);
    org.apache.hadoop.hive.metastore.api.ColumnStatisticsData  statsWrapper =
            new org.apache.hadoop.hive.metastore.api.ColumnStatisticsData();
    statsWrapper.setStringStats(statsData);
    return statsWrapper;
  }

  public static org.apache.hadoop.hive.metastore.api.ColumnStatistics getHiveTableColumnStatistics() {
    org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc columnStatisticsDesc =
            new org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc();
    columnStatisticsDesc.setDbName("database-name");
    columnStatisticsDesc.setTableName("table-name");
    columnStatisticsDesc.setIsTblLevel(true);
    columnStatisticsDesc.setLastAnalyzed(12345);

    org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj decimalObj =
            new org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj();
    decimalObj.setColName("decimal-column");
    decimalObj.setColType("decimal(9,6)");
    decimalObj.setStatsData(ObjectTestUtils.getHiveDecimalColumnStatsData());

    org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj longObj =
            new org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj();
    longObj.setColName("long-column");
    longObj.setColType("integer");
    longObj.setStatsData(ObjectTestUtils.getHiveLongColumnStatsData());

    org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics =
            new org.apache.hadoop.hive.metastore.api.ColumnStatistics();
    columnStatistics.setStatsDesc(columnStatisticsDesc);
    columnStatistics.setStatsObj(Arrays.asList(decimalObj, longObj));

    return columnStatistics;
  }

  public static org.apache.hadoop.hive.metastore.api.ColumnStatistics getHivePartitionColumnStatistics() {
    org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics = getHiveTableColumnStatistics();
    columnStatistics.getStatsDesc().setIsTblLevel(false);
    columnStatistics.getStatsDesc().setPartName("A=a/B=b");

    return columnStatistics;
  }

  public static PartitionError getPartitionError(List<String> values, Exception exception) {
    return new PartitionError()
        .withPartitionValues(values)
        .withErrorDetail(new ErrorDetail()
            .withErrorCode(exception.getClass().getSimpleName())
            .withErrorMessage(exception.getMessage()));
  }

  public static HiveObjectRef getHiveObjectRef() {
    HiveObjectRef obj = new HiveObjectRef();
    obj.setObjectType(HiveObjectType.TABLE);
    obj.setDbName("default");
    obj.setObjectName("foo");
    return obj;
  }

  public static PrivilegeBag getPrivilegeBag() {
    PrivilegeBag bag = new PrivilegeBag();
    HiveObjectPrivilege hivePrivilege = new HiveObjectPrivilege();
    hivePrivilege.setPrincipalName("user1");
    hivePrivilege.setPrincipalType(org.apache.hadoop.hive.metastore.api.PrincipalType.USER);
    org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo grantInfo =
        new org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo();
    grantInfo.setGrantor("user2");
    grantInfo.setGrantorType(org.apache.hadoop.hive.metastore.api.PrincipalType.USER);
    hivePrivilege.setGrantInfo(grantInfo);
    bag.setPrivileges(Lists.newArrayList(hivePrivilege));
    return bag;
  }

  public static org.apache.hadoop.hive.metastore.api.Order getTestOrder() {
    org.apache.hadoop.hive.metastore.api.Order order = new org.apache.hadoop.hive.metastore.api.Order();
    order.setCol("foo");
    order.setOrder(1);
    return order;
  }

  public static Role getTestRole() {
    Role role = new Role();
    role.setRoleName("test-role");
    role.setOwnerName("owner");
    return role;
  }

}
