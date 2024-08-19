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

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.expressions.Expressions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestManifestCleanup extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @TestTemplate
  public void testDelete() {
    assertThat(listManifestFiles()).isEmpty();

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Table should have one append manifest")
        .hasSize(1);

    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Table should have one delete manifest")
        .hasSize(1);

    table.newAppend().commit();

    assertThat(table.currentSnapshot().allManifests(table.io())).isEmpty();
  }

  @TestTemplate
  public void testPartialDelete() {
    assertThat(listManifestFiles()).isEmpty();

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot s1 = table.currentSnapshot();
    assertThat(s1.allManifests(table.io())).as("Table should have one append manifest").hasSize(1);

    table.newDelete().deleteFile(FILE_B).commit();

    Snapshot s2 = table.currentSnapshot();
    assertThat(s2.allManifests(table.io())).as("Table should have one mixed manifest").hasSize(1);

    table.newAppend().commit();

    Snapshot s3 = table.currentSnapshot();
    assertThat(s3.allManifests(table.io())).isEqualTo(s2.allManifests(table.io()));
  }

  @TestTemplate
  public void testOverwrite() {
    assertThat(listManifestFiles()).isEmpty();

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Table should have one append manifest")
        .hasSize(1);

    table
        .newOverwrite()
        .overwriteByRowFilter(Expressions.alwaysTrue())
        .addFile(FILE_C)
        .addFile(FILE_D)
        .commit();

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Table should have one delete manifest and one append manifest")
        .hasSize(2);

    table
        .newOverwrite()
        .overwriteByRowFilter(Expressions.alwaysTrue())
        .addFile(FILE_A)
        .addFile(FILE_B)
        .commit();

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Table should have one delete manifest and one append manifest")
        .hasSize(2);
  }
}
