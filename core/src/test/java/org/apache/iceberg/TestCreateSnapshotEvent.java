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

import java.util.Arrays;
import java.util.Collection;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.events.Listeners;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestCreateSnapshotEvent extends TestBase {
  @Parameters(name = "formatVersion={0}")
  public static Collection<Integer> parameters() {
    return Arrays.asList(1, 2);
  }

  private CreateSnapshotEvent currentEvent;

  public TestCreateSnapshotEvent(int formatVersion) {
    super(formatVersion);
    Listeners.register(new MyListener(), CreateSnapshotEvent.class);
  }

  @TestTemplate
  public void testAppendCommitEvent() {
    Assertions.assertThat(listManifestFiles()).as("Table should start empty").isEmpty();

    table.newAppend().appendFile(FILE_A).commit();
    Assertions.assertThat(currentEvent).isNotNull();
    Assertions.assertThat(currentEvent.summary().get("added-records"))
        .as("Added records in the table should be 1")
        .isEqualTo("1");
    Assertions.assertThat(currentEvent.summary().get("added-data-files"))
        .as("Added files in the table should be 1")
        .isEqualTo("1");
    Assertions.assertThat(currentEvent.summary().get("total-records"))
        .as("Total records in the table should be 1")
        .isEqualTo("1");
    Assertions.assertThat(currentEvent.summary().get("total-data-files"))
        .as("Total data files in the table should be 1")
        .isEqualTo("1");

    table.newAppend().appendFile(FILE_A).commit();
    Assertions.assertThat(currentEvent).isNotNull();
    Assertions.assertThat(currentEvent.summary().get("added-records"))
        .as("Added records in the table should be 1")
        .isEqualTo("1");
    Assertions.assertThat(currentEvent.summary().get("added-data-files"))
        .as("Added files in the table should be 1")
        .isEqualTo("1");
    Assertions.assertThat(currentEvent.summary().get("total-records"))
        .as("Total records in the table should be 2")
        .isEqualTo("2");
    Assertions.assertThat(currentEvent.summary().get("total-data-files"))
        .as("Total data files in the table should be 2")
        .isEqualTo("2");
  }

  @TestTemplate
  public void testAppendAndDeleteCommitEvent() {
    Assertions.assertThat(listManifestFiles()).as("Table should start empty").isEmpty();

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    Assertions.assertThat(currentEvent).as("Current event should not be null").isNotNull();
    Assertions.assertThat(currentEvent.summary().get("added-records"))
        .as("Added records in the table should be 2")
        .isEqualTo("2");
    Assertions.assertThat(currentEvent.summary().get("added-data-files"))
        .as("Added files in the table should be 2")
        .isEqualTo("2");
    Assertions.assertThat(currentEvent.summary().get("total-records"))
        .as("Total records in the table should be 2")
        .isEqualTo("2");
    Assertions.assertThat(currentEvent.summary().get("total-data-files"))
        .as("Total data files in the table should be 2")
        .isEqualTo("2");

    table.newDelete().deleteFile(FILE_A).commit();
    Assertions.assertThat(currentEvent)
        .as("Current event should not be null after delete")
        .isNotNull();
    Assertions.assertThat(currentEvent.summary().get("deleted-records"))
        .as("Deleted records in the table should be 1")
        .isEqualTo("1");
    Assertions.assertThat(currentEvent.summary().get("deleted-data-files"))
        .as("Deleted files in the table should be 1")
        .isEqualTo("1");
    Assertions.assertThat(currentEvent.summary().get("total-records"))
        .as("Total records in the table should be 1")
        .isEqualTo("1");
    Assertions.assertThat(currentEvent.summary().get("total-data-files"))
        .as("Total data files in the table should be 1")
        .isEqualTo("1");
  }

  class MyListener implements Listener<CreateSnapshotEvent> {
    @Override
    public void notify(CreateSnapshotEvent event) {
      currentEvent = event;
    }
  }
}
