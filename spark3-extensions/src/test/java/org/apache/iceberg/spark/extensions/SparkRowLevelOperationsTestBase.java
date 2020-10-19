/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.spark.extensions;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public abstract class SparkRowLevelOperationsTestBase extends SparkExtensionsTestBase {

  private static final Random RANDOM = ThreadLocalRandom.current();

  protected final String fileFormat;
  protected final boolean vectorized;

  public SparkRowLevelOperationsTestBase(String catalogName, String implementation,
                                         Map<String, String> config, String fileFormat,
                                         boolean vectorized) {
    super(catalogName, implementation, config);
    this.fileFormat = fileFormat;
    this.vectorized = vectorized;
  }

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, format = {3}, vectorized = {4}")
  public static Object[][] parameters() {
    return new Object[][] {
        { "testhive", SparkCatalog.class.getName(),
            ImmutableMap.of(
                "type", "hive",
                "default-namespace", "default"
            ),
            "orc",
            true
        },
        { "testhadoop", SparkCatalog.class.getName(),
            ImmutableMap.of(
                "type", "hadoop"
            ),
            "parquet",
            RANDOM.nextBoolean()
        },
        { "spark_catalog", SparkSessionCatalog.class.getName(),
            ImmutableMap.of(
                "type", "hive",
                "default-namespace", "default",
                "parquet-enabled", "true",
                "cache-enabled", "false" // Spark will delete tables using v1, leaving the cache out of sync
            ),
            "avro",
            false
        }
    };
  }
}
