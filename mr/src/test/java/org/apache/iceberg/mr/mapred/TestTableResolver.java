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

package org.apache.iceberg.mr.mapred;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestTableResolver {

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private Schema schema = new Schema(required(1, "string_field", Types.StringType.get()));
  private File tableLocation;

  @Before
  public void before() throws IOException, SerDeException {
    tableLocation = tmp.newFolder();
    Configuration conf = new Configuration();
    HadoopTables tables = new HadoopTables(conf);
    tables.create(schema, PartitionSpec.unpartitioned(), Collections.emptyMap(), tableLocation.toString());
  }

  @Test
  public void resolveTableFromConfigurationDefault() throws IOException {
    Configuration conf = new Configuration();
    conf.set(InputFormatConfig.TABLE_LOCATION, tableLocation.getAbsolutePath());

    Table table = TableResolver.resolveTableFromConfiguration(conf);
    Assert.assertEquals(tableLocation.getAbsolutePath(), table.location());
  }

  @Test
  public void resolveTableFromConfigurationHadoopTables() throws IOException {
    Configuration conf = new Configuration();
    conf.set(InputFormatConfig.CATALOG_NAME, InputFormatConfig.HADOOP_TABLES);
    conf.set(InputFormatConfig.TABLE_LOCATION, tableLocation.getAbsolutePath());

    Table table = TableResolver.resolveTableFromConfiguration(conf);
    Assert.assertEquals(tableLocation.getAbsolutePath(), table.location());
  }

  @Test(expected = NullPointerException.class)
  public void resolveTableFromConfigurationHadoopTablesNoLocation() throws IOException {
    Configuration conf = new Configuration();
    conf.set(InputFormatConfig.CATALOG_NAME, InputFormatConfig.HADOOP_TABLES);

    TableResolver.resolveTableFromConfiguration(conf);
  }

  @Test(expected = NoSuchNamespaceException.class)
  public void resolveTableFromConfigurationInvalidName() throws IOException {
    Configuration conf = new Configuration();
    conf.set(InputFormatConfig.CATALOG_NAME, "invalid-name");

    TableResolver.resolveTableFromConfiguration(conf);
  }

  @Test
  public void resolveTableFromJobConfDefault() throws IOException {
    JobConf conf = new JobConf();
    conf.set(InputFormatConfig.TABLE_LOCATION, tableLocation.getAbsolutePath());

    Table table = TableResolver.resolveTableFromConfiguration(conf);
    Assert.assertEquals(tableLocation.getAbsolutePath(), table.location());
  }

  @Test
  public void resolveTableFromPropertiesDefault() throws IOException {
    Configuration conf = new Configuration();
    Properties properties = new Properties();
    properties.setProperty(InputFormatConfig.TABLE_LOCATION, tableLocation.getAbsolutePath());

    Table table = TableResolver.resolveTableFromConfiguration(conf, properties);
    Assert.assertEquals(tableLocation.getAbsolutePath(), table.location());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void resolveTableFromConfigurationHiveCatalog() throws IOException {
    Configuration conf = new Configuration();
    conf.set(InputFormatConfig.CATALOG_NAME, InputFormatConfig.HIVE_CATALOG);
    conf.set(InputFormatConfig.TABLE_NAME, "table_a");

    TableResolver.resolveTableFromConfiguration(conf);
  }

  @Test(expected = NullPointerException.class)
  public void resolveTableFromConfigurationHiveCatalogMissingTableName() throws IOException {
    Configuration conf = new Configuration();
    conf.set(InputFormatConfig.CATALOG_NAME, InputFormatConfig.HIVE_CATALOG);

    TableResolver.resolveTableFromConfiguration(conf);
  }

}
