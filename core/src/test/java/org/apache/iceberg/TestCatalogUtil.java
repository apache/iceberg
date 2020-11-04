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

package org.apache.iceberg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Assert;
import org.junit.Test;

public class TestCatalogUtil {

  @Test
  public void loadCustomCatalog() {
    Map<String, String> options = new HashMap<>();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";
    Catalog catalog = CatalogUtil.loadCatalog(TestCatalog.class.getName(), name, options, hadoopConf);
    Assert.assertTrue(catalog instanceof TestCatalog);
    Assert.assertEquals(name, ((TestCatalog) catalog).catalogName);
    Assert.assertEquals(options, ((TestCatalog) catalog).flinkOptions);
  }

  @Test
  public void loadCustomCatalog_withHadoopConfig() {
    Map<String, String> options = new HashMap<>();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("key", "val");
    String name = "custom";
    Catalog catalog = CatalogUtil.loadCatalog(TestCatalogConfigurable.class.getName(), name, options, hadoopConf);
    Assert.assertTrue(catalog instanceof TestCatalogConfigurable);
    Assert.assertEquals(name, ((TestCatalogConfigurable) catalog).catalogName);
    Assert.assertEquals(options, ((TestCatalogConfigurable) catalog).flinkOptions);
    Assert.assertEquals(hadoopConf, ((TestCatalogConfigurable) catalog).configuration);
  }

  @Test
  public void loadCustomCatalog_NoArgConstructorNotFound() {
    Map<String, String> options = new HashMap<>();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";
    AssertHelpers.assertThrows("must have no-arg constructor",
        IllegalArgumentException.class,
        "missing no-arg constructor",
        () -> CatalogUtil.loadCatalog(TestCatalogBadConstructor.class.getName(), name, options, hadoopConf));
  }

  @Test
  public void loadCustomCatalog_NotImplementCatalog() {
    Map<String, String> options = new HashMap<>();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";

    AssertHelpers.assertThrows("must implement catalog",
        IllegalArgumentException.class,
        "does not implement Catalog",
        () -> CatalogUtil.loadCatalog(TestCatalogNoInterface.class.getName(), name, options, hadoopConf));
  }

  public static class TestCatalog extends BaseMetastoreCatalog {

    private String catalogName;
    private Map<String, String> flinkOptions;

    public TestCatalog() {
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
      this.catalogName = name;
      this.flinkOptions = properties;
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return null;
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return false;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {

    }
  }

  public static class TestCatalogConfigurable extends BaseMetastoreCatalog implements Configurable {

    private String catalogName;
    private Map<String, String> flinkOptions;
    private Configuration configuration;

    public TestCatalogConfigurable() {
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
      this.catalogName = name;
      this.flinkOptions = properties;
    }

    @Override
    public void setConf(Configuration conf) {
      this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
      return configuration;
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return null;
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return false;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {

    }
  }

  public static class TestCatalogBadConstructor extends BaseMetastoreCatalog {

    public TestCatalogBadConstructor(String arg) {
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return null;
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return false;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {

    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
    }
  }

  public static class TestCatalogNoInterface {
    public TestCatalogNoInterface() {
    }
  }
}
