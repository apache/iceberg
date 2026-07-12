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
package org.apache.iceberg.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.actions.ValidateTableIntegrity.ValidateScope;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the Immutables-generated {@link ValidateTableIntegrity.Result} and {@link
 * ValidateScope#fromString}.
 */
public class TestValidateTableIntegrityResult {

  @Test
  public void emptyResultIsValid() {
    ValidateTableIntegrity.Result result =
        ImmutableValidateTableIntegrity.Result.builder().build();
    assertThat(result.isValid()).isTrue();
    assertThat(result.missingFileCount()).isEqualTo(0);
    assertThat(result.totalMetadataFiles()).isEqualTo(0);
    assertThat(result.totalDataFiles()).isEqualTo(0);
    assertThat(result.totalDeleteFiles()).isEqualTo(0);
  }

  @Test
  public void missingMetadataFilesAreReported() {
    ValidateTableIntegrity.Result result =
        ImmutableValidateTableIntegrity.Result.builder()
            .totalMetadataFiles(10)
            .missingMetadataFiles(
                Arrays.asList(
                    "s3://bucket/metadata/manifest1.avro", "s3://bucket/metadata/manifest2.avro"))
            .build();
    assertThat(result.isValid()).isFalse();
    assertThat(result.missingFileCount()).isEqualTo(2);
  }

  @Test
  public void missingDataFilesAreReported() {
    ValidateTableIntegrity.Result result =
        ImmutableValidateTableIntegrity.Result.builder()
            .totalDataFiles(100)
            .missingDataFiles(
                Arrays.asList(
                    "s3://bucket/data/file1.parquet",
                    "s3://bucket/data/file2.parquet",
                    "s3://bucket/data/file3.parquet"))
            .build();
    assertThat(result.isValid()).isFalse();
    assertThat(result.missingFileCount()).isEqualTo(3);
  }

  @Test
  public void missingDeleteFilesAreReported() {
    ValidateTableIntegrity.Result result =
        ImmutableValidateTableIntegrity.Result.builder()
            .totalDeleteFiles(5)
            .missingDeleteFiles(Collections.singletonList("s3://bucket/data/delete-file1.parquet"))
            .build();
    assertThat(result.isValid()).isFalse();
    assertThat(result.missingFileCount()).isEqualTo(1);
  }

  @Test
  public void mixedMissingFilesSumCorrectly() {
    ValidateTableIntegrity.Result result =
        ImmutableValidateTableIntegrity.Result.builder()
            .missingMetadataFiles(Collections.singletonList("s3://bucket/metadata/manifest1.avro"))
            .missingDataFiles(
                Arrays.asList("s3://bucket/data/file1.parquet", "s3://bucket/data/file2.parquet"))
            .missingDeleteFiles(Collections.singletonList("s3://bucket/data/delete-file1.parquet"))
            .build();
    assertThat(result.isValid()).isFalse();
    assertThat(result.missingFileCount()).isEqualTo(4);
  }

  @Test
  public void validationSummaryRendersForValidResult() {
    ValidateTableIntegrity.Result result =
        ImmutableValidateTableIntegrity.Result.builder()
            .totalMetadataFiles(10)
            .totalDataFiles(100)
            .totalDeleteFiles(5)
            .build();
    assertThat(result.validationSummary())
        .contains("Total Metadata Files Validated: 10")
        .contains("Missing Metadata Files: 0")
        .contains("Total Data Files Validated: 100")
        .contains("Missing Data Files: 0")
        .contains("Total Delete Files Validated: 5")
        .contains("Missing Delete Files: 0")
        .contains("Status: PASSED");
  }

  @Test
  public void validationSummaryRendersForInvalidResult() {
    ValidateTableIntegrity.Result result =
        ImmutableValidateTableIntegrity.Result.builder()
            .totalMetadataFiles(10)
            .missingMetadataFiles(Arrays.asList("manifest1.avro", "manifest2.avro"))
            .totalDataFiles(100)
            .missingDataFiles(Collections.singletonList("file1.parquet"))
            .totalDeleteFiles(5)
            .missingDeleteFiles(Collections.singletonList("delete1.parquet"))
            .build();
    assertThat(result.validationSummary())
        .contains("Missing Metadata Files: 2")
        .contains("Missing Data Files: 1")
        .contains("Missing Delete Files: 1")
        .contains("Status: FAILED");
  }

  @Test
  public void missingFilePathsArePreserved() {
    List<String> metadataFiles = Collections.singletonList("s3://bucket/metadata/v1.metadata.json");
    List<String> dataFiles =
        Arrays.asList("s3://bucket/data/part-00001.parquet", "s3://bucket/data/part-00002.parquet");
    List<String> deleteFiles = Collections.singletonList("s3://bucket/data/delete-00001.parquet");

    ValidateTableIntegrity.Result result =
        ImmutableValidateTableIntegrity.Result.builder()
            .missingMetadataFiles(metadataFiles)
            .missingDataFiles(dataFiles)
            .missingDeleteFiles(deleteFiles)
            .build();

    assertThat(result.missingMetadataFiles()).isEqualTo(metadataFiles);
    assertThat(result.missingDataFiles()).isEqualTo(dataFiles);
    assertThat(result.missingDeleteFiles()).isEqualTo(deleteFiles);
    assertThat(result.missingFileCount()).isEqualTo(4);
  }

  @Test
  public void handlesLargeMissingFileLists() {
    List<String> largeList = Lists.newArrayListWithExpectedSize(1000);
    for (int i = 0; i < 1000; i++) {
      largeList.add("file.parquet");
    }
    ValidateTableIntegrity.Result result =
        ImmutableValidateTableIntegrity.Result.builder()
            .totalDataFiles(10000)
            .missingDataFiles(largeList)
            .build();
    assertThat(result.isValid()).isFalse();
    assertThat(result.missingFileCount()).isEqualTo(1000);
    assertThat(result.missingDataFiles()).hasSize(1000);
  }

  @Test
  public void validateScopeParsesCaseInsensitively() {
    assertThat(ValidateScope.fromString("all")).isEqualTo(ValidateScope.ALL);
    assertThat(ValidateScope.fromString("latest")).isEqualTo(ValidateScope.LATEST);
    assertThat(ValidateScope.fromString("ALL")).isEqualTo(ValidateScope.ALL);
    assertThat(ValidateScope.fromString("Latest")).isEqualTo(ValidateScope.LATEST);
  }

  @Test
  public void validateScopeThrowsOnUnknownValue() {
    assertThatThrownBy(() -> ValidateScope.fromString("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Must be one of: all, latest");
  }

  @Test
  public void validateScopeThrowsOnNoneValue() {
    assertThatThrownBy(() -> ValidateScope.fromString("none"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid validateTableIntegrity scope");
  }
}
