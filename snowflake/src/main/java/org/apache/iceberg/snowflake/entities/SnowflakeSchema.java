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
package org.apache.iceberg.snowflake.entities;

import java.util.List;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class SnowflakeSchema {
  private String name;
  private String databaseName;

  public SnowflakeSchema(String databaseName, String name) {
    this.databaseName = databaseName;
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public String getDatabase() {
    return databaseName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof SnowflakeSchema)) {
      return false;
    }

    SnowflakeSchema that = (SnowflakeSchema) o;
    return Objects.equal(this.databaseName, that.databaseName)
        && Objects.equal(this.name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(databaseName, name);
  }

  @Override
  public String toString() {
    return String.format("%s.%s", databaseName, name);
  }

  public static ResultSetHandler<List<SnowflakeSchema>> createHandler() {
    return rs -> {
      List<SnowflakeSchema> schemas = Lists.newArrayList();
      while (rs.next()) {
        String databaseName = rs.getString("database_name");
        String name = rs.getString("name");
        schemas.add(new SnowflakeSchema(databaseName, name));
      }
      return schemas;
    };
  }
}
