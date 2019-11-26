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

package org.apache.iceberg.spark.source;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.spark.SparkUtils;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class IcebergTableProvider implements DataSourceRegister, TableProvider {
  @Override
  public String shortName() {
    return "iceberg";
  }

  @Override
  public Table getTable(CaseInsensitiveStringMap options) {
    return getTable(options, null);
  }

  @Override
  public Table getTable(CaseInsensitiveStringMap options, StructType readSchema) {
    // Get Iceberg table from options
    Configuration conf = new Configuration(SparkUtils.getBaseConf());
    org.apache.iceberg.Table tableInIceberg = getTableAndResolveHadoopConfiguration(options, conf);

    // Build Spark table based on Iceberg table, and return it
    return new IcebergTable(tableInIceberg, readSchema);
  }

  protected org.apache.iceberg.Table getTableAndResolveHadoopConfiguration(
      CaseInsensitiveStringMap options, Configuration conf) {
    // Overwrite configurations from the Spark Context with configurations from the options.
    mergeIcebergHadoopConfs(conf, options);

    // Find table (in Iceberg) based on the given path
    org.apache.iceberg.Table table = findTable(options, conf);

    // Set confs from table properties
    mergeIcebergHadoopConfs(conf, table.properties());

    // Re-overwrite values set in options and table properties but were not in the environment.
    mergeIcebergHadoopConfs(conf, options);

    return table;
  }

  /**
   * Merge delta options into base conf
   *
   * @param baseConf the base conf
   * @param options  the delta options to merge into base
   */
  private void mergeIcebergHadoopConfs(Configuration baseConf, Map<String, String> options) {
    options.keySet().stream()
        .filter(key -> key.startsWith("hadoop."))  /* filter all keys staring with "hadoop." */
        .forEach(key -> baseConf.set(key.replaceFirst("hadoop.", ""), options.get(key)));
    /* Modify the key by removing the prefix of "hadoop." and merge into base */
  }

  protected org.apache.iceberg.Table findTable(CaseInsensitiveStringMap options, Configuration conf) {
    Optional<String> path = Optional.ofNullable(options.get("path"));
    Preconditions.checkArgument(path.isPresent(), "Cannot open table: path is not set");

    if (path.get().contains("/")) {  // hadoop table
      HadoopTables tables = new HadoopTables(conf);
      return tables.load(path.get());
    } else {  // hive table
      HiveCatalog hiveCatalog = HiveCatalogs.loadCatalog(conf);
      TableIdentifier tableIdentifier = TableIdentifier.parse(path.get());
      return hiveCatalog.loadTable(tableIdentifier);
    }
  }
}
