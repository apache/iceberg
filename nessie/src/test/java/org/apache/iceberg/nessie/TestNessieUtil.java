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
package org.apache.iceberg.nessie;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.GenericMetadata;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableIcebergTable;

public class TestNessieUtil {

  @Test
  public void testTableMetadataJsonRoundtrip() {
    // Construct a dummy TableMetadata object
    Map<String, String> properties = Collections.singletonMap("property-key", "property-value");
    String location = "obj://foo/bar/baz";
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(
            new Schema(1, NestedField.of(1, false, "column", StringType.get())),
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            location,
            properties);

    // Produce a generic JsonNode from the TableMetadata
    JsonNode jsonNode = NessieUtil.tableMetadataAsJsonNode(tableMetadata);
    Assertions.assertThat(jsonNode)
        .asInstanceOf(InstanceOfAssertFactories.type(JsonNode.class))
        .extracting(
            n -> n.get("format-version").asLong(-2L),
            n -> n.get("location").asText("x"),
            n -> n.get("properties").get("property-key").asText())
        .containsExactly(1L, location, "property-value");

    // Create a Nessie IcebergTable object with the JsonNode as the metadata
    IcebergTable icebergTableNoMetadata = IcebergTable.of(location, 0L, 1, 2, 3, "cid");
    IcebergTable icebergTable =
        ImmutableIcebergTable.builder()
            .from(icebergTableNoMetadata)
            .metadata(GenericMetadata.of("iceberg", jsonNode))
            .build();

    // Deserialize the TableMetadata from Nessie IcebergTable
    TableMetadata deserializedMetadata =
        NessieUtil.tableMetadataFromIcebergTable(null, icebergTable, location);

    // (Could compare tableMetadata against deserializedMetadata, but TableMetadata has no equals())

    // Produce a JsonNode from the deserializedMetadata and compare that against jsonNode
    JsonNode deserializedJsonNode = NessieUtil.tableMetadataAsJsonNode(deserializedMetadata);
    Assertions.assertThat(deserializedJsonNode).isEqualTo(jsonNode);
  }

  @Test
  public void testTableMetadataFromFileIO() {
    String location = "obj://foo/bar/baz";
    FileIO fileIoMock = Mockito.mock(FileIO.class);
    IcebergTable icebergTableNoMetadata = IcebergTable.of(location, 0L, 1, 2, 3, "cid");

    // Check that newInputFile() is called when IcebergTable has no metadata
    Mockito.when(fileIoMock.newInputFile(location))
        .thenThrow(new RuntimeException("newInputFile called"));
    Assertions.assertThatThrownBy(
            () ->
                NessieUtil.tableMetadataFromIcebergTable(
                    fileIoMock, icebergTableNoMetadata, location))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("newInputFile called");
  }

  @Test
  public void testBuildingCommitMetadataWithNullCatalogOptions() {
    Assertions.assertThatThrownBy(() -> NessieUtil.buildCommitMetadata("msg", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("catalogOptions must not be null");
  }

  @Test
  public void testSparkAppIdAndUserIsSetOnCommitMetadata() {
    String commitMsg = "commit msg";
    String appId = "SPARK_ID_123";
    String user = "sparkUser";
    CommitMeta commitMeta =
        NessieUtil.buildCommitMetadata(
            commitMsg,
            ImmutableMap.of(CatalogProperties.APP_ID, appId, CatalogProperties.USER, user));
    Assertions.assertThat(commitMeta.getMessage()).isEqualTo(commitMsg);
    Assertions.assertThat(commitMeta.getAuthor()).isEqualTo(user);
    Assertions.assertThat(commitMeta.getProperties()).hasSize(2);
    Assertions.assertThat(commitMeta.getProperties().get(NessieUtil.APPLICATION_TYPE))
        .isEqualTo("iceberg");
    Assertions.assertThat(commitMeta.getProperties().get(CatalogProperties.APP_ID))
        .isEqualTo(appId);
  }

  @Test
  public void testAuthorIsSetOnCommitMetadata() {
    String commitMsg = "commit msg";
    CommitMeta commitMeta = NessieUtil.buildCommitMetadata(commitMsg, ImmutableMap.of());
    Assertions.assertThat(commitMeta.getMessage()).isEqualTo(commitMsg);
    Assertions.assertThat(commitMeta.getAuthor()).isEqualTo(System.getProperty("user.name"));
    Assertions.assertThat(commitMeta.getProperties()).hasSize(1);
    Assertions.assertThat(commitMeta.getProperties().get(NessieUtil.APPLICATION_TYPE))
        .isEqualTo("iceberg");
  }

  @Test
  public void testAuthorIsNullWithoutJvmUser() {
    String jvmUserName = System.getProperty("user.name");
    try {
      System.clearProperty("user.name");
      CommitMeta commitMeta = NessieUtil.buildCommitMetadata("commit msg", ImmutableMap.of());
      Assertions.assertThat(commitMeta.getAuthor()).isNull();
    } finally {
      System.setProperty("user.name", jvmUserName);
    }
  }
}
