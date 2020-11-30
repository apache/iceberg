/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.spark.source;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.SparkSession;

public final class SetupSourceCatalog {

  private SetupSourceCatalog() {

  }

  public static void setupSparkCatalog(SparkSession spark) {
    setupSparkCatalog(spark, SparkSessionCatalog.class.getName());
  }

  public static void setupSparkCatalog(SparkSession spark, String catalogName) {
    ImmutableMap<String, String> config = ImmutableMap.of(
        "type", "hive",
        "default-namespace", "default",
        "parquet-enabled", "true",
        "cache-enabled", "false"
    );
    spark.conf().set("spark.sql.catalog.spark_catalog", catalogName);
    config.forEach((key, value) -> spark.conf().set("spark.sql.catalog.spark_catalog." + key, value));
  }
}
