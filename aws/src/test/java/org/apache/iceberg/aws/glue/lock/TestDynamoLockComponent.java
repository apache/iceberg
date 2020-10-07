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

import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestDynamoLockComponent {

  @Test
  public void testSerializationFromHive() {
    LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db");
    lockComponent.setTablename("table");
    DynamoLockComponent dynamoLockComponent = DynamoLockComponent.fromHive(lockComponent);
    assertEquals("db", dynamoLockComponent.getDbName());
    assertEquals("table", dynamoLockComponent.getTableName());
    assertEquals(LockType.EXCLUSIVE, dynamoLockComponent.getLockType());
    assertEquals(LockLevel.TABLE, dynamoLockComponent.getLockLevel());
  }

  @Test
  public void testSerDesRoundTrip() {
    LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db");
    lockComponent.setTablename("table");
    DynamoLockComponent dynamoLockComponent = DynamoLockComponent.fromJson(
        DynamoLockComponent.fromHive(lockComponent).toString());
    assertEquals("db", dynamoLockComponent.getDbName());
    assertEquals("table", dynamoLockComponent.getTableName());
    assertEquals(LockType.EXCLUSIVE, dynamoLockComponent.getLockType());
    assertEquals(LockLevel.TABLE, dynamoLockComponent.getLockLevel());
  }
}
