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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.events.Listeners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestCreateSnapshotEvent extends TestBase {

  private CreateSnapshotEvent currentEvent;

  @BeforeEach
  public void initListener() {
    Listeners.register(new MyListener(), CreateSnapshotEvent.class);
  }

  @TestTemplate
  public void testAppendCommitEvent() {
    assertThat(listManifestFiles()).as("Table should start empty").isEmpty();

    table.newAppend().appendFile(FILE_A).commit();
    assertThat(currentEvent).isNotNull();
    assertThat(currentEvent.summary())
        .containsEntry("added-records", "1")
        .containsEntry("added-data-files", "1")
        .containsEntry("total-records", "1")
        .containsEntry("total-data-files", "1");

    table.newAppend().appendFile(FILE_A).commit();
    assertThat(currentEvent).isNotNull();
    assertThat(currentEvent.summary())
        .containsEntry("added-records", "1")
        .containsEntry("added-data-files", "1")
        .containsEntry("total-records", "2")
        .containsEntry("total-data-files", "2");
  }

  @TestTemplate
  public void testAppendAndDeleteCommitEvent() {
    assertThat(listManifestFiles()).as("Table should start empty").isEmpty();

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    assertThat(currentEvent).as("Current event should not be null").isNotNull();
    assertThat(currentEvent.summary())
        .containsEntry("added-records", "2")
        .containsEntry("added-data-files", "2")
        .containsEntry("total-records", "2")
        .containsEntry("total-data-files", "2");

    table.newDelete().deleteFile(FILE_A).commit();
    assertThat(currentEvent).as("Current event should not be null after delete").isNotNull();
    assertThat(currentEvent.summary())
        .containsEntry("deleted-records", "1")
        .containsEntry("deleted-data-files", "1")
        .containsEntry("total-records", "1")
        .containsEntry("total-data-files", "1");
  }

  class MyListener implements Listener<CreateSnapshotEvent> {
    @Override
    public void notify(CreateSnapshotEvent event) {
      currentEvent = event;
    }
  }
}
