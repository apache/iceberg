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
package org.apache.iceberg;

import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.events.Listeners;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestCreateSnapshotEvent extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  private CreateSnapshotEvent currentEvent;

  public TestCreateSnapshotEvent(int formatVersion) {
    super(formatVersion);
    Listeners.register(new MyListener(), CreateSnapshotEvent.class);
  }

  @Test
  public void testAppendCommitEvent() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend().appendFile(FILE_A).commit();
    Assert.assertNotNull(currentEvent);
    Assert.assertEquals(
        "Added records in the table should be 1", "1", currentEvent.summary().get("added-records"));
    Assert.assertEquals(
        "Added files in the table should be 1",
        "1",
        currentEvent.summary().get("added-data-files"));
    Assert.assertEquals(
        "Total records in the table should be 1", "1", currentEvent.summary().get("total-records"));
    Assert.assertEquals(
        "Total data files in the table should be 1",
        "1",
        currentEvent.summary().get("total-data-files"));

    table.newAppend().appendFile(FILE_A).commit();
    Assert.assertNotNull(currentEvent);
    Assert.assertEquals(
        "Added records in the table should be 1", "1", currentEvent.summary().get("added-records"));
    Assert.assertEquals(
        "Added files in the table should be 1",
        "1",
        currentEvent.summary().get("added-data-files"));
    Assert.assertEquals(
        "Total records in the table should be 2", "2", currentEvent.summary().get("total-records"));
    Assert.assertEquals(
        "Total data files in the table should be 2",
        "2",
        currentEvent.summary().get("total-data-files"));
  }

  @Test
  public void testAppendAndDeleteCommitEvent() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    Assert.assertNotNull(currentEvent);
    Assert.assertEquals(
        "Added records in the table should be 2", "2", currentEvent.summary().get("added-records"));
    Assert.assertEquals(
        "Added files in the table should be 2",
        "2",
        currentEvent.summary().get("added-data-files"));
    Assert.assertEquals(
        "Total records in the table should be 2", "2", currentEvent.summary().get("total-records"));
    Assert.assertEquals(
        "Total data files in the table should be 2",
        "2",
        currentEvent.summary().get("total-data-files"));

    table.newDelete().deleteFile(FILE_A).commit();
    Assert.assertNotNull(currentEvent);
    Assert.assertEquals(
        "Deleted records in the table should be 1",
        "1",
        currentEvent.summary().get("deleted-records"));
    Assert.assertEquals(
        "Deleted files in the table should be 1",
        "1",
        currentEvent.summary().get("deleted-data-files"));
    Assert.assertEquals(
        "Total records in the table should be 1", "1", currentEvent.summary().get("total-records"));
    Assert.assertEquals(
        "Total data files in the table should be 1",
        "1",
        currentEvent.summary().get("total-data-files"));
  }

  class MyListener implements Listener<CreateSnapshotEvent> {
    @Override
    public void notify(CreateSnapshotEvent event) {
      currentEvent = event;
    }
  }
}
