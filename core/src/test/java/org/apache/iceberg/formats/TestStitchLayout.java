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
package org.apache.iceberg.formats;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestStitchLayout {
  private static final Types.NestedField ID = required(1, "id", Types.IntegerType.get());
  private static final Types.NestedField DATA = optional(2, "data", Types.StringType.get());
  private static final Types.NestedField CATEGORY = optional(3, "category", Types.StringType.get());
  private static final List<Schema> VERTICAL_SPLITS =
      List.of(new Schema(DATA), new Schema(ID, CATEGORY));

  @Test
  void locatesFieldsInProjectionOrder() {
    StitchLayout layout = StitchLayout.of(new Schema(ID, DATA, CATEGORY), VERTICAL_SPLITS);

    assertThat(layout.size()).isEqualTo(3);
    assertThat(layout.split(0)).isEqualTo(1);
    assertThat(layout.ordinal(0)).isEqualTo(0);
    assertThat(layout.split(1)).isEqualTo(0);
    assertThat(layout.ordinal(1)).isEqualTo(0);
    assertThat(layout.split(2)).isEqualTo(1);
    assertThat(layout.ordinal(2)).isEqualTo(1);
  }

  @Test
  void rejectsFieldProvidedBySeveralSplits() {
    List<Schema> verticalSplits = List.of(new Schema(ID, DATA), new Schema(DATA, CATEGORY));

    assertThatThrownBy(() -> StitchLayout.of(new Schema(ID, DATA, CATEGORY), verticalSplits))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot stitch field 2: data");
  }

  @Test
  void rejectsFieldMissingFromSplits() {
    List<Schema> verticalSplits = List.of(new Schema(DATA), new Schema(ID));

    assertThatThrownBy(() -> StitchLayout.of(new Schema(ID, DATA, CATEGORY), verticalSplits))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot stitch field 3: category");
  }
}
