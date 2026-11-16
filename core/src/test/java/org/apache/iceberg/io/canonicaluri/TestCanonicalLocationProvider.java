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
package org.apache.iceberg.io.canonicaluri;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.TableProperties;
import org.junit.jupiter.api.Test;

class TestCanonicalLocationProvider {
  @Test
  void producesCanonicalDataLocation() {
    CanonicalLocationProvider provider =
        new CanonicalLocationProvider("s3://warehouse/db/table", new HashMap<>());

    assertThat(provider.newDataLocation("a.parquet"))
        .isEqualTo("s3://warehouse/db/table/data/a.parquet");
  }

  @Test
  void permitsCanonicalExternalDataRoot() {
    Map<String, String> properties = new HashMap<>();
    properties.put(PathAliasProperties.LOCATION_PROVIDER_CANONICAL_PREFIX, "s3://warehouse/");
    properties.put(TableProperties.WRITE_DATA_LOCATION, "s3://warehouse/shared/table-data");

    CanonicalLocationProvider provider =
        new CanonicalLocationProvider("s3://warehouse/db/table", properties);

    assertThat(provider.newDataLocation("a.parquet"))
        .isEqualTo("s3://warehouse/shared/table-data/a.parquet");
  }

  @Test
  void rejectsPhysicalOrEscapingDataRoot() {
    Map<String, String> physical = new HashMap<>();
    physical.put(TableProperties.WRITE_DATA_LOCATION, "ofs://om/s3v/warehouse/table/data");
    assertThatThrownBy(() -> new CanonicalLocationProvider("s3://warehouse/table", physical))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("outside the configured canonical namespace");

    CanonicalLocationProvider provider =
        new CanonicalLocationProvider("s3://warehouse/table", new HashMap<>());
    assertThatThrownBy(() -> provider.newDataLocation("../secret"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("path traversal");
  }

  @Test
  void rejectsPhysicalMetadataRoot() {
    Map<String, String> properties = new HashMap<>();
    properties.put(TableProperties.WRITE_METADATA_LOCATION, "ofs://om/s3v/warehouse/metadata");

    assertThatThrownBy(() -> new CanonicalLocationProvider("s3://warehouse/table", properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("outside the configured canonical namespace");
  }
}
