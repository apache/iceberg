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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.DeletesReadTest;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkReadDeletes extends DeletesReadTest {
  private FlinkSource.Builder builder;
  private HadoopCatalog catalog;
  private final FileFormat format;

  @Before
  public void prepareData() throws IOException {
    Configuration conf = new Configuration();
    File warehouseFile = temp.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    String warehouse = "file:" + warehouseFile;
    catalog = new HadoopCatalog(conf, warehouse);
    builder = FlinkSource.forRowData().tableLoader(TableLoader.fromHadoopTable(warehouse + "/default/table"));

    this.table = createTable("table", SCHEMA, SPEC);
    generateTestData();
    table.newAppend()
        .appendFile(dataFile)
        .commit();
  }

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { FileFormat.PARQUET },
        new Object[] { FileFormat.AVRO },
        new Object[] { FileFormat.ORC }
    };
  }

  public TestFlinkReadDeletes(FileFormat inputFormat) {
    this.format = inputFormat;
  }

  @Override
  public Table createTable(String name, Schema schema, PartitionSpec spec) {
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    Table table = catalog.createTable(TableIdentifier.of("default", name), schema, spec, props);
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    return table;
  }

  @Override
  public StructLikeSet rowSet(Table tbl, String... columns) throws IOException {
    Schema projected = table.schema().select(columns);
    RowType rowType = FlinkSchemaUtil.convert(projected);
    FlinkInputFormat inputFormat = builder.project(FlinkSchemaUtil.toSchema(rowType)).buildFormat();

    StructLikeSet set = StructLikeSet.create(projected.asStruct());
    TestFlinkInputFormat.getRows(inputFormat).forEach(row -> {
      RowDataWrapper wrapper = new RowDataWrapper(rowType, projected.asStruct());
      set.add(wrapper.wrap(row));
    });

    return set;
  }
}
