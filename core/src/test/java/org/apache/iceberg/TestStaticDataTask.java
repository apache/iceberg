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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestStaticDataTask {

  private static final Schema TEST_SCHEMA = new Schema(required(1, "value", Types.LongType.get()));

  @Test
  public void ofStringPathDoesNotReadFile() {
    // Even a clearly-bogus path must be tolerated: the new overload promises no I/O against the
    // location, only preserving it for identification. If the implementation regressed to building
    // the DataFile via withInputFile (which calls InputFile.getLength()), this call would fail
    // when the FileIO eventually tried to HEAD the path.
    String bogusLocation = "file:///does/not/exist/never/HEADed.metadata.json";
    List<Long> values = ImmutableList.of(1L, 2L, 3L);

    DataTask task =
        StaticDataTask.of(
            bogusLocation, TEST_SCHEMA, TEST_SCHEMA, values, v -> StaticDataTask.Row.of(v));

    assertThat(task.isDataTask()).as("should be a DataTask").isTrue();
    assertThat(task.length()).as("length must be 0 (no file bytes processed)").isEqualTo(0L);
    assertThat(task.file().fileSizeInBytes())
        .as("DataFile size must be 0 (no HEAD on synthetic file)")
        .isEqualTo(0L);
    assertThat(task.file().format())
        .as("DataFile format must be METADATA")
        .isEqualTo(FileFormat.METADATA);
    assertThat(task.file().location())
        .as("DataFile path must preserve the supplied location for identification")
        .isEqualTo(bogusLocation);
    assertThat(task.file().recordCount())
        .as("DataFile record count must reflect the row count")
        .isEqualTo(values.size());
  }

  @Test
  public void ofStringPathRowsProjectCorrectly() {
    String location = "file:///irrelevant/path.metadata.json";
    List<Long> values = ImmutableList.of(10L, 20L, 30L);

    DataTask task =
        StaticDataTask.of(
            location, TEST_SCHEMA, TEST_SCHEMA, values, v -> StaticDataTask.Row.of(v));

    try (CloseableIterable<StructLike> rows = task.rows()) {
      List<Long> read =
          ImmutableList.copyOf(
              org.apache.iceberg.relocated.com.google.common.collect.Iterables.transform(
                  rows, row -> row.get(0, Long.class)));
      assertThat(read).containsExactlyElementsOf(values);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void ofStringPathWithNoValuesYieldsEmptyTask() {
    String location = "file:///irrelevant/path.metadata.json";

    DataTask task =
        StaticDataTask.of(
            location,
            TEST_SCHEMA,
            TEST_SCHEMA,
            ImmutableList.<Long>of(),
            v -> StaticDataTask.Row.of(v));

    assertThat(task.length()).isEqualTo(0L);
    assertThat(task.file().recordCount()).isEqualTo(0L);

    try (CloseableIterable<StructLike> rows = task.rows()) {
      assertThat(rows).isEmpty();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
