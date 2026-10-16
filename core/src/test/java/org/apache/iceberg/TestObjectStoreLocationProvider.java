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

import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestObjectStoreLocationProvider {
  @ParameterizedTest
  @CsvSource({
    "s3://bucket/db/table, db/table",
    "s3://bucket/db/table/, db/table",
    "s3://bucket/ns/db/table, db/table",
    "s3://bucket/table, table",
    "s3://bucket/a//table, table",
    "file:/db/table, db/table",
    "file:///db/table, db/table",
    "file:///table, table"
  })
  void pathContext(String tableLocation, String expectedContext) {
    String location =
        new LocationProviders.ObjectStoreLocationProvider(
                tableLocation, Map.of(TableProperties.WRITE_DATA_LOCATION, "s3://other/data"))
            .newDataLocation("test.parquet");

    assertThat(location)
        .startsWith("s3://other/data/")
        .endsWith(String.format("/%s/test.parquet", expectedContext));
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3://bucket", "s3://bucket/", "file:/", "file:///"})
  void noPathContext(String tableLocation) {
    String location =
        new LocationProviders.ObjectStoreLocationProvider(
                tableLocation, Map.of(TableProperties.WRITE_DATA_LOCATION, "s3://other/data"))
            .newDataLocation("test.parquet");

    assertThat(location).matches("s3://other/data/[01]{4}/[01]{4}/[01]{4}/[01]{8}/test\\.parquet");
  }
}
