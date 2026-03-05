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
package org.apache.iceberg.flink.source;

import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class SqlHelpers {
  private SqlHelpers() {}

  public static List<Row> sql(TableEnvironment tableEnv, String query, Object... args) {
    TableResult tableResult = tableEnv.executeSql(String.format(query, args));
    try (CloseableIterator<Row> iter = tableResult.collect()) {
      List<Row> results = Lists.newArrayList(iter);
      return results;
    } catch (Exception e) {
      throw new RuntimeException("Failed to collect table result", e);
    }
  }

  public static String sqlOptionsToString(Map<String, String> sqlOptions) {
    StringBuilder builder = new StringBuilder();
    sqlOptions.forEach((key, value) -> builder.append(optionToKv(key, value)).append(","));
    String optionStr = builder.toString();
    if (optionStr.endsWith(",")) {
      optionStr = optionStr.substring(0, optionStr.length() - 1);
    }

    if (!optionStr.isEmpty()) {
      optionStr = String.format("/*+ OPTIONS(%s)*/", optionStr);
    }

    return optionStr;
  }

  private static String optionToKv(String key, Object value) {
    return "'" + key + "'='" + value + "'";
  }
}
