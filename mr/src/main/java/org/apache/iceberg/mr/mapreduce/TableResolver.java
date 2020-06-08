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

package org.apache.iceberg.mr.mapreduce;

import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

final class TableResolver {

  private TableResolver() {
  }

  public static Table findTable(Configuration conf) {
    String path = conf.get(InputFormatConfig.TABLE_PATH);
    Preconditions.checkArgument(path != null, "Table path should not be null");
    if (path.contains("/")) {
      HadoopTables tables = new HadoopTables(conf);
      return tables.load(path);
    }

    String catalogFuncClass = conf.get(InputFormatConfig.CATALOG);
    if (catalogFuncClass != null) {
      Function<Configuration, Catalog> catalogFunc = (Function<Configuration, Catalog>)
              DynConstructors.builder(Function.class)
                      .impl(catalogFuncClass)
                      .build()
                      .newInstance();
      Catalog catalog = catalogFunc.apply(conf);
      TableIdentifier tableIdentifier = TableIdentifier.parse(path);
      return catalog.loadTable(tableIdentifier);
    } else {
      throw new IllegalArgumentException("No custom catalog specified to load table " + path);
    }
  }

}
