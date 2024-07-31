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
package org.apache.iceberg.flink.maintenance.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.iceberg.flink.TableLoader;
import org.junit.jupiter.api.Test;

class TestTagBasedLockFactory extends TestLockFactoryBase {
  @Override
  TriggerLockFactory lockFactory() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    return new TagBasedLockFactory(tableLoader);
  }

  @Test
  void testTryLockWithEmptyTable() throws IOException {
    sql.exec("CREATE TABLE %s (id int, data varchar)", "empty_table");

    TableLoader tableLoader = sql.tableLoader("empty_table");
    try (TriggerLockFactory lockFactory = new TagBasedLockFactory(tableLoader)) {
      lockFactory.open();
      TriggerLockFactory.Lock lock = lockFactory.createLock();
      assertThat(lock.tryLock()).isTrue();
    }
  }
}
