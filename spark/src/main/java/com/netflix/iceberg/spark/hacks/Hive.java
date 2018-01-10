/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.spark.hacks;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition;
import org.apache.spark.sql.hive.HiveUtils$;
import org.apache.spark.sql.hive.client.HiveClient;
import scala.Option;
import scala.collection.Seq;
import java.util.List;

public class Hive {
  public static Seq<CatalogTablePartition> partitions(SparkSession spark, String name) {
    List<String> parts = Lists.newArrayList(Splitter.on('.').limit(2).split(name));
    String db = parts.size() == 1 ? "default" : parts.get(0);
    String table = parts.get(parts.size() == 1 ? 0 : 1);

    HiveClient client = HiveUtils$.MODULE$.newClientForMetadata(
        spark.sparkContext().conf(),
        spark.sparkContext().hadoopConfiguration());
    client.getPartitions(db, table, Option.empty());

    return client.getPartitions(db, table, Option.empty());
  }
}
