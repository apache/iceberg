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

import java.io.IOException;
import java.util.Map;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.DeleteReadTests;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkInputFormatReaderDeletes extends DeleteReadTests {
  private static HiveConf hiveConf = null;
  private static HiveCatalog catalog = null;
  private static TestHiveMetastore metastore = null;

  private final FileFormat format;

  @Parameterized.Parameters(name = "fileFormat={0}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { FileFormat.PARQUET },
        new Object[] { FileFormat.AVRO },
        new Object[] { FileFormat.ORC }
    };
  }

  public TestFlinkInputFormatReaderDeletes(FileFormat inputFormat) {
    this.format = inputFormat;
  }

  @BeforeClass
  public static void startMetastore() {
    TestFlinkInputFormatReaderDeletes.metastore = new TestHiveMetastore();
    metastore.start();
    TestFlinkInputFormatReaderDeletes.hiveConf = metastore.hiveConf();
    TestFlinkInputFormatReaderDeletes.catalog = new HiveCatalog(hiveConf);
  }

  @AfterClass
  public static void stopMetastore() {
    metastore.stop();
    catalog.close();
    TestFlinkInputFormatReaderDeletes.catalog = null;
  }

  @Override
  protected Table createTable(String name, Schema schema, PartitionSpec spec) {
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.DEFAULT_FILE_FORMAT, format.name());

    Table table = catalog.createTable(TableIdentifier.of("default", name), schema, spec, props);
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    return table;
  }

  @Override
  protected void dropTable(String name) {
    catalog.dropTable(TableIdentifier.of("default", name));
  }

  @Override
  protected StructLikeSet rowSet(String name, Table testTable, String... columns) throws IOException {
    Schema projected = testTable.schema().select(columns);
    RowType rowType = FlinkSchemaUtil.convert(projected);
    CatalogLoader hiveCatalogLoader = CatalogLoader.hive(catalog.name(),
        hiveConf,
        hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname),
        hiveConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
        hiveConf.getInt("iceberg.hive.client-pool-size", 5)
    );
    FlinkInputFormat inputFormat = FlinkSource.forRowData()
        .tableLoader(TableLoader.fromCatalog(hiveCatalogLoader, TableIdentifier.of("default", name)))
        .project(FlinkSchemaUtil.toSchema(rowType)).buildFormat();

    StructLikeSet set = StructLikeSet.create(projected.asStruct());
    TestHelpers.readRowData(inputFormat, rowType).forEach(rowData -> {
      RowDataWrapper wrapper = new RowDataWrapper(rowType, projected.asStruct());
      set.add(wrapper.wrap(rowData));
    });

    return set;
  }

  @Override
  protected boolean expectPruned() {
    return false;
  }
}
