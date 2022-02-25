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

package org.apache.iceberg.spark;

import java.util.Map;
import org.apache.iceberg.TestCatalogUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class TestSparkCatalogEndToEnd extends SparkTestBaseWithCatalog {
  public static Integer notifyTimes = 0;

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {{
        TestSparkCatalogConfig.TEST.catalogName(),
        TestSparkCatalogConfig.TEST.implementation(),
        TestSparkCatalogConfig.TEST.properties()
        }
    };
  }

  public TestSparkCatalogEndToEnd(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void createTable() {
    TestCatalogUtil.TestListener.NOTIFY_TIMES.set(0);
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
  }

  @After
  public void removeTable() {
    TestCatalogUtil.TestListener.NOTIFY_TIMES.set(0);
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testEndToEnd() throws NoSuchTableException {
    TestCatalogUtil.TestListener.NOTIFY_TIMES.set(0);

    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
    Assert.assertEquals(1, TestCatalogUtil.TestListener.NOTIFY_TIMES.get());

    sql("SELECT * FROM %s", tableName);
    Assert.assertEquals(2, TestCatalogUtil.TestListener.NOTIFY_TIMES.get());
    TestCatalogUtil.TestListener.NOTIFY_TIMES.set(0);
  }

  public enum TestSparkCatalogConfig {
    TEST("hive", SparkCatalog.class.getName(), ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            "listeners.listenerOne.impl", TestCatalogUtil.TestListener.class.getName(),
            "listeners.listenerOne.test.info", "Information",
            "listeners.listenerOne.test.client", "Client-Info"
    ));

    private final String catalogName;
    private final String implementation;
    private final Map<String, String> properties;

    TestSparkCatalogConfig(String catalogName, String implementation, Map<String, String> properties) {
      this.catalogName = catalogName;
      this.implementation = implementation;
      this.properties = properties;
    }

    public String catalogName() {
      return catalogName;
    }

    public String implementation() {
      return implementation;
    }

    public Map<String, String> properties() {
      return properties;
    }
  }
}
