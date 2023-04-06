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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestMetadataUtil extends TableTestBase {

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {1}, new Object[] {2},
    };
  }

  public TestMetadataUtil(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testMetadataLocationWithNullTable() {
    assertThatThrownBy(() -> MetadataUtil.metadataLocation((Table) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid table: null");
  }

  @Test
  public void testMetadataLocationWithNullMetadata() {
    assertThatThrownBy(() -> MetadataUtil.metadataLocation((TableMetadata) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid metadata: null");
  }

  @Test
  public void testMetadataLocationWithWriteMetadataLocationProperty() {
    String expectedLocation = "s3://bucket/metadata";
    this.table
        .updateProperties()
        .set(TableProperties.WRITE_METADATA_LOCATION, expectedLocation)
        .commit();

    assertThat(MetadataUtil.metadataLocation(table)).isEqualTo(expectedLocation);
  }

  @Test
  public void testMetadataLocationWithoutWriteMetadataLocationProperty() {
    String tableLocation = table.location();
    assertThat(MetadataUtil.metadataLocation(table)).isEqualTo(tableLocation + "/metadata");
  }

  @Test
  public void testNewMetadataLocationWithNullTable() {
    assertThatThrownBy(() -> MetadataUtil.newMetadataLocation((Table) null, "filename"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid table: null");
  }

  @Test
  public void testNewMetadataLocationWithNullFilename() {
    assertThatThrownBy(() -> MetadataUtil.newMetadataLocation(table, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid filename: null");
  }

  @Test
  public void testNewMetadataLocation() {
    String tableLocation = "s3://bucket/table";
    String filename = "metadata.json";
    table.updateLocation().setLocation(tableLocation).commit();
    assertThat(MetadataUtil.newMetadataLocation(table, filename))
        .isEqualTo(tableLocation + "/metadata/" + filename);
  }

  @Test
  public void testNewMetadataLocationWithMetadata() {
    String tableLocation = "s3://bucket/table";
    String filename = "metadata.json";
    table.updateLocation().setLocation(tableLocation).commit();
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    assertThat(MetadataUtil.newMetadataLocation(metadata, filename))
        .isEqualTo(tableLocation + "/metadata/" + filename);
  }
}
