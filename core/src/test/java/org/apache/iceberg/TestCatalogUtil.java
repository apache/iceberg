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

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCatalogUtil {

  @Test
  public void loadCustomCatalog() {
    Map<String, String> options = Maps.newHashMap();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";
    Catalog catalog =
        CatalogUtil.loadCatalog(TestCatalog.class.getName(), name, options, hadoopConf);
    Assertions.assertThat(catalog).isInstanceOf(TestCatalog.class);
    Assertions.assertThat(((TestCatalog) catalog).catalogName).isEqualTo(name);
    Assertions.assertThat(((TestCatalog) catalog).catalogProperties).isEqualTo(options);
  }

  @Test
  public void loadCustomCatalog_withHadoopConfig() {
    Map<String, String> options = Maps.newHashMap();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("key", "val");
    String name = "custom";
    Catalog catalog =
        CatalogUtil.loadCatalog(TestCatalogConfigurable.class.getName(), name, options, hadoopConf);
    Assertions.assertThat(catalog).isInstanceOf(TestCatalogConfigurable.class);
    Assertions.assertThat(((TestCatalogConfigurable) catalog).catalogName).isEqualTo(name);
    Assertions.assertThat(((TestCatalogConfigurable) catalog).catalogProperties).isEqualTo(options);
    Assertions.assertThat(((TestCatalogConfigurable) catalog).configuration).isEqualTo(hadoopConf);
  }

  @Test
  public void loadCustomCatalog_NoArgConstructorNotFound() {
    Map<String, String> options = Maps.newHashMap();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";
    Assertions.assertThatThrownBy(
            () ->
                CatalogUtil.loadCatalog(
                    TestCatalogBadConstructor.class.getName(), name, options, hadoopConf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot initialize Catalog implementation")
        .hasMessageContaining(
            "NoSuchMethodException: org.apache.iceberg.TestCatalogUtil$TestCatalogBadConstructor.<init>()");
  }

  @Test
  public void loadCustomCatalog_NotImplementCatalog() {
    Map<String, String> options = Maps.newHashMap();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";

    Assertions.assertThatThrownBy(
            () ->
                CatalogUtil.loadCatalog(
                    TestCatalogNoInterface.class.getName(), name, options, hadoopConf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot initialize Catalog")
        .hasMessageContaining("does not implement Catalog");
  }

  @Test
  public void loadCustomCatalog_ConstructorErrorCatalog() {
    Map<String, String> options = Maps.newHashMap();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";

    String impl = TestCatalogErrorConstructor.class.getName();
    Assertions.assertThatThrownBy(() -> CatalogUtil.loadCatalog(impl, name, options, hadoopConf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot initialize Catalog implementation")
        .hasMessageContaining("NoClassDefFoundError: Error while initializing class");
  }

  @Test
  public void loadCustomCatalog_BadCatalogNameCatalog() {
    Map<String, String> options = Maps.newHashMap();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";
    String impl = "CatalogDoesNotExist";
    Assertions.assertThatThrownBy(() -> CatalogUtil.loadCatalog(impl, name, options, hadoopConf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot initialize Catalog implementation")
        .hasMessageContaining("java.lang.ClassNotFoundException: CatalogDoesNotExist");
  }

  @Test
  public void loadCustomFileIO_noArg() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key", "val");
    FileIO fileIO = CatalogUtil.loadFileIO(TestFileIONoArg.class.getName(), properties, null);
    Assertions.assertThat(fileIO).isInstanceOf(TestFileIONoArg.class);
    Assertions.assertThat(((TestFileIONoArg) fileIO).map).isEqualTo(properties);
  }

  @Test
  public void loadCustomFileIO_hadoopConfigConstructor() {
    Configuration configuration = new Configuration();
    configuration.set("key", "val");
    FileIO fileIO =
        CatalogUtil.loadFileIO(HadoopFileIO.class.getName(), Maps.newHashMap(), configuration);
    Assertions.assertThat(fileIO).isInstanceOf(HadoopFileIO.class);
    Assertions.assertThat(((HadoopFileIO) fileIO).conf().get("key")).isEqualTo("val");
  }

  @Test
  public void loadCustomFileIO_configurable() {
    Configuration configuration = new Configuration();
    configuration.set("key", "val");
    FileIO fileIO =
        CatalogUtil.loadFileIO(
            TestFileIOConfigurable.class.getName(), Maps.newHashMap(), configuration);
    Assertions.assertThat(fileIO).isInstanceOf(TestFileIOConfigurable.class);
    Assertions.assertThat(((TestFileIOConfigurable) fileIO).configuration).isEqualTo(configuration);
  }

  @Test
  public void loadCustomFileIO_badArg() {
    Assertions.assertThatThrownBy(
            () -> CatalogUtil.loadFileIO(TestFileIOBadArg.class.getName(), Maps.newHashMap(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Cannot initialize FileIO implementation "
                + "org.apache.iceberg.TestCatalogUtil$TestFileIOBadArg: Cannot find constructor");
  }

  @Test
  public void loadCustomFileIO_badClass() {
    Assertions.assertThatThrownBy(
            () ->
                CatalogUtil.loadFileIO(TestFileIONotImpl.class.getName(), Maps.newHashMap(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot initialize FileIO")
        .hasMessageContaining("does not implement FileIO");
  }

  @Test
  public void buildCustomCatalog_withTypeSet() {
    Map<String, String> options = Maps.newHashMap();
    options.put(CatalogProperties.CATALOG_IMPL, "CustomCatalog");
    options.put(CatalogUtil.ICEBERG_CATALOG_TYPE, "hive");
    Configuration hadoopConf = new Configuration();
    String name = "custom";
    Assertions.assertThatThrownBy(() -> CatalogUtil.buildIcebergCatalog(name, options, hadoopConf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot create catalog custom, both type and catalog-impl are set: type=hive, catalog-impl=CustomCatalog");
  }

  @Test
  public void loadCustomMetricsReporter_noArg() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key", "val");
    properties.put(
        CatalogProperties.METRICS_REPORTER_IMPL, TestMetricsReporterDefault.class.getName());

    MetricsReporter metricsReporter = CatalogUtil.loadMetricsReporter(properties);
    Assertions.assertThat(metricsReporter).isInstanceOf(TestMetricsReporterDefault.class);
  }

  @Test
  public void loadCustomMetricsReporter_badArg() {
    Assertions.assertThatThrownBy(
            () ->
                CatalogUtil.loadMetricsReporter(
                    ImmutableMap.of(
                        CatalogProperties.METRICS_REPORTER_IMPL,
                        TestMetricsReporterBadArg.class.getName())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing no-arg constructor");
  }

  @Test
  public void loadCustomMetricsReporter_badClass() {
    Assertions.assertThatThrownBy(
            () ->
                CatalogUtil.loadMetricsReporter(
                    ImmutableMap.of(
                        CatalogProperties.METRICS_REPORTER_IMPL,
                        TestFileIONotImpl.class.getName())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not implement MetricsReporter");
  }

  @Test
  public void fullTableNameWithDifferentValues() {
    String uriTypeCatalogName = "thrift://host:port/db.table";
    String namespace = "ns";
    String nameSpaceWithTwoLevels = "ns.l2";
    String tableName = "tbl";
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
    Assertions.assertThat(CatalogUtil.fullTableName(uriTypeCatalogName, tableIdentifier))
        .isEqualTo(uriTypeCatalogName + "/" + namespace + "." + tableName);

    tableIdentifier = TableIdentifier.of(nameSpaceWithTwoLevels, tableName);
    Assertions.assertThat(CatalogUtil.fullTableName(uriTypeCatalogName, tableIdentifier))
        .isEqualTo(uriTypeCatalogName + "/" + nameSpaceWithTwoLevels + "." + tableName);

    Assertions.assertThat(CatalogUtil.fullTableName(uriTypeCatalogName + "/", tableIdentifier))
        .isEqualTo(uriTypeCatalogName + "/" + nameSpaceWithTwoLevels + "." + tableName);

    String nonUriCatalogName = "test.db.catalog";
    Assertions.assertThat(CatalogUtil.fullTableName(nonUriCatalogName, tableIdentifier))
        .isEqualTo(nonUriCatalogName + "." + nameSpaceWithTwoLevels + "." + tableName);

    String pathStyleCatalogName = "/test/db";
    Assertions.assertThat(CatalogUtil.fullTableName(pathStyleCatalogName, tableIdentifier))
        .isEqualTo(pathStyleCatalogName + "/" + nameSpaceWithTwoLevels + "." + tableName);
  }

  @Test
  public void testLoadMetricsReporterFromThreadContextClassLoader() throws Exception {
    URL jarUrl =
        getClass()
            .getClassLoader()
            .getResource("com/example/target/custom-metrics-reporter-1.0-SNAPSHOT.jar");
    Assertions.assertThat(jarUrl).isNotNull();
    ClassLoader customClassLoader =
        new URLClassLoader(new URL[] {jarUrl}, ClassLoader.getSystemClassLoader());
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.METRICS_REPORTER_IMPL, "com.example.CustomMetricsReporter");

    // Temporarily change the context class loader to the custom class loader
    ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      try {
        CatalogUtil.loadMetricsReporter(properties);
        Assertions.fail("Expected to throw IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        Assertions.assertThat(e.getMessage()).contains("Cannot initialize MetricsReporter");
      }
      Thread.currentThread().setContextClassLoader(customClassLoader);
      MetricsReporter reporter = CatalogUtil.loadMetricsReporter(properties);

      Assertions.assertThat(reporter).isNotNull().isInstanceOf(MetricsReporter.class);
      Assertions.assertThat(reporter instanceof MetricsReporter).isTrue();

      PrintStream originalOut = System.out;
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream newOut = new PrintStream(baos);
        System.setOut(newOut);
        reporter.report(null);
        String sysOut = baos.toString();
        Assertions.assertThat(sysOut)
            .contains("com.example.CustomMetricsReporter: Report executed");
      } finally {
        System.setOut(originalOut);
      }
    } finally {
      Thread.currentThread().setContextClassLoader(originalContextClassLoader);
    }
  }

  public static class TestCatalog extends BaseMetastoreCatalog {

    private String catalogName;
    private Map<String, String> catalogProperties;

    public TestCatalog() {}

    @Override
    public void initialize(String name, Map<String, String> properties) {
      this.catalogName = name;
      this.catalogProperties = properties;
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
    public void renameTable(TableIdentifier from, TableIdentifier to) {}
  }

  public static class TestCatalogConfigurable extends BaseMetastoreCatalog implements Configurable {

    private String catalogName;
    private Map<String, String> catalogProperties;
    private Configuration configuration;

    public TestCatalogConfigurable() {}

    @Override
    public void initialize(String name, Map<String, String> properties) {
      this.catalogName = name;
      this.catalogProperties = properties;
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
    public void renameTable(TableIdentifier from, TableIdentifier to) {}
  }

  public static class TestCatalogBadConstructor extends BaseMetastoreCatalog {

    public TestCatalogBadConstructor(String arg) {}

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
    public void renameTable(TableIdentifier from, TableIdentifier to) {}

    @Override
    public void initialize(String name, Map<String, String> properties) {}
  }

  public static class TestCatalogNoInterface {
    public TestCatalogNoInterface() {}
  }

  public static class TestFileIOConfigurable implements FileIO, Configurable {

    private Configuration configuration;

    public TestFileIOConfigurable() {}

    @Override
    public void setConf(Configuration conf) {
      this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
      return configuration;
    }

    @Override
    public InputFile newInputFile(String path) {
      return null;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return null;
    }

    @Override
    public void deleteFile(String path) {}

    public Configuration getConfiguration() {
      return configuration;
    }
  }

  public static class TestFileIONoArg implements FileIO {

    private Map<String, String> map;

    public TestFileIONoArg() {}

    @Override
    public InputFile newInputFile(String path) {
      return null;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return null;
    }

    @Override
    public void deleteFile(String path) {}

    public Map<String, String> getMap() {
      return map;
    }

    @Override
    public void initialize(Map<String, String> properties) {
      map = properties;
    }
  }

  public static class TestFileIOBadArg implements FileIO {

    private final String arg;

    public TestFileIOBadArg(String arg) {
      this.arg = arg;
    }

    @Override
    public InputFile newInputFile(String path) {
      return null;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return null;
    }

    @Override
    public void deleteFile(String path) {}

    public String getArg() {
      return arg;
    }
  }

  public static class TestFileIONotImpl {
    public TestFileIONotImpl() {}
  }

  public static class TestMetricsReporterBadArg implements MetricsReporter {
    private final String arg;

    public TestMetricsReporterBadArg(String arg) {
      this.arg = arg;
    }

    @Override
    public void report(MetricsReport report) {}
  }

  public static class TestMetricsReporterDefault implements MetricsReporter {

    @Override
    public void report(MetricsReport report) {}
  }
}
