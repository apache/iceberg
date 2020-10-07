/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.aws.glue.lock;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.IOException;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.codehaus.jackson.map.ObjectMapper;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DynamoLockComponent {

  // TODO: reuse the JsonUtil by moving it to iceberg-common?
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private LockType lockType;
  private LockLevel lockLevel;
  private String dbName;
  private String tableName;
  private String partitionName;

  public static DynamoLockComponent fromHive(LockComponent lockComponent) {
    DynamoLockComponent component = new DynamoLockComponent();
    component.setDbName(lockComponent.getDbname());
    component.setTableName(lockComponent.getTablename());
    component.setPartitionName(lockComponent.getPartitionname());
    component.setLockLevel(lockComponent.getLevel());
    component.setLockType(lockComponent.getType());
    return component;
  }

  public static DynamoLockComponent fromJson(String json) {
    try {
      return MAPPER.readValue(json, DynamoLockComponent.class);
    } catch (IOException e) {
      // simply throw as runtime exception
      throw new RuntimeException("fail to serialize DynamoLockComponent", e);
    }

  }

  @Override
  public String toString() {
    try {
      return MAPPER.writeValueAsString(this);
    } catch (IOException e) {
      // simply throw as runtime exception
      throw new RuntimeException("fail to serialize DynamoLockComponent", e);
    }
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public void setPartitionName(String partitionName) {
    this.partitionName = partitionName;
  }

  public LockLevel getLockLevel() {
    return lockLevel;
  }

  public void setLockLevel(LockLevel lockLevel) {
    this.lockLevel = lockLevel;
  }

  public LockType getLockType() {
    return lockType;
  }

  public void setLockType(LockType lockType) {
    this.lockType = lockType;
  }
}
