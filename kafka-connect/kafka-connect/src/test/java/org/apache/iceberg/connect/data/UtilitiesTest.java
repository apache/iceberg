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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class UtilitiesTest {

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
    Catalog result = Utilities.loadCatalog(config);

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
    Catalog result = Utilities.loadCatalog(config);

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

  @Test
  public void testExtractFromRecordValueStruct() {
    Schema valSchema = SchemaBuilder.struct().field("key", Schema.INT64_SCHEMA).build();
    Struct val = new Struct(valSchema).put("key", 123L);
    Object result = Utilities.extractFromRecordValue(val, "key");
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueStructNested() {
    Schema idSchema = SchemaBuilder.struct().field("key", Schema.INT64_SCHEMA).build();
    Schema dataSchema = SchemaBuilder.struct().field("id", idSchema).build();
    Schema valSchema = SchemaBuilder.struct().field("data", dataSchema).build();

    Struct id = new Struct(idSchema).put("key", 123L);
    Struct data = new Struct(dataSchema).put("id", id);
    Struct val = new Struct(valSchema).put("data", data);

    Object result = Utilities.extractFromRecordValue(val, "data.id.key");
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueStructNull() {
    Schema valSchema = SchemaBuilder.struct().field("key", Schema.INT64_SCHEMA).build();
    Struct val = new Struct(valSchema).put("key", 123L);

    Object result = Utilities.extractFromRecordValue(val, "");
    assertThat(result).isNull();

    result = Utilities.extractFromRecordValue(val, "xkey");
    assertThat(result).isNull();
  }

  @Test
  public void testExtractFromRecordValueMap() {
    Map<String, Object> val = ImmutableMap.of("key", 123L);
    Object result = Utilities.extractFromRecordValue(val, "key");
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueMapNested() {
    Map<String, Object> id = ImmutableMap.of("key", 123L);
    Map<String, Object> data = ImmutableMap.of("id", id);
    Map<String, Object> val = ImmutableMap.of("data", data);

    Object result = Utilities.extractFromRecordValue(val, "data.id.key");
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueMapNull() {
    Map<String, Object> val = ImmutableMap.of("key", 123L);

    Object result = Utilities.extractFromRecordValue(val, "");
    assertThat(result).isNull();

    result = Utilities.extractFromRecordValue(val, "xkey");
    assertThat(result).isNull();
  }
}
