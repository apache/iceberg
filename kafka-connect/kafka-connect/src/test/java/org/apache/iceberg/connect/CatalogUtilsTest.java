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
package org.apache.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class CatalogUtilsTest {

  private static final String HADOOP_CONF_TEMPLATE =
      "<configuration><property><name>%s</name><value>%s</value></property></configuration>";

  @TempDir private Path tempDir;

  public static class TestCatalog extends InMemoryCatalog implements Configurable<Configuration> {
    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }
  }

  @Test
  public void testLoadCatalogNoHadoopDir() {
    Map<String, String> props =
        ImmutableMap.of(
            "topics",
            "mytopic",
            "iceberg.tables",
            "mytable",
            "iceberg.hadoop.conf-prop",
            "conf-value",
            "iceberg.catalog.catalog-impl",
            TestCatalog.class.getName());
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    Catalog result = CatalogUtils.loadCatalog(config);

    assertThat(result).isInstanceOf(TestCatalog.class);

    Configuration conf = ((TestCatalog) result).conf;
    assertThat(conf).isNotNull();

    // check that the sink config property was added
    assertThat(conf.get("conf-prop")).isEqualTo("conf-value");

    // check that core-site.xml was loaded
    assertThat(conf.get("foo")).isEqualTo("bar");
  }

  @ParameterizedTest
  @ValueSource(strings = {"core-site.xml", "hdfs-site.xml", "hive-site.xml"})
  public void testLoadCatalogWithHadoopDir(String confFile) throws IOException {
    Path path = tempDir.resolve(confFile);
    String xml = String.format(HADOOP_CONF_TEMPLATE, "file-prop", "file-value");
    Files.write(path, xml.getBytes(StandardCharsets.UTF_8));

    Map<String, String> props =
        ImmutableMap.of(
            "topics",
            "mytopic",
            "iceberg.tables",
            "mytable",
            "iceberg.hadoop-conf-dir",
            tempDir.toString(),
            "iceberg.hadoop.conf-prop",
            "conf-value",
            "iceberg.catalog.catalog-impl",
            TestCatalog.class.getName());
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    Catalog result = CatalogUtils.loadCatalog(config);

    assertThat(result).isInstanceOf(TestCatalog.class);

    Configuration conf = ((TestCatalog) result).conf;
    assertThat(conf).isNotNull();

    // check that the sink config property was added
    assertThat(conf.get("conf-prop")).isEqualTo("conf-value");

    // check that the config file was loaded
    assertThat(conf.get("file-prop")).isEqualTo("file-value");

    // check that core-site.xml was loaded
    assertThat(conf.get("foo")).isEqualTo("bar");
  }
}
