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

package org.apache.iceberg.hive;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class TestHiveCommits extends HiveTableBaseTest {

  @Test
  public void testSuppressUnlockExceptions() throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    Assert.assertNotEquals(metadataV1.file().location(), metadataV2.file().location());

    HiveTableOperations spyOps = spy(ops);
    doThrow(new RuntimeException()).when(spyOps).doUnlock(anyLong());
    spyOps.commit(metadataV2, metadataV1);

    ops.refresh();

    // the commit must succeed
    Assert.assertEquals(metadataV1.file().location(), ops.current().file().location());
  }
}
