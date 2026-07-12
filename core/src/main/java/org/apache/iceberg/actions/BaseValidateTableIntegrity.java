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

import java.util.List;
import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.immutables.value.Value;

@Value.Enclosing
@SuppressWarnings("ImmutablesStyle")
@Value.Style(
    typeImmutableEnclosing = "ImmutableValidateTableIntegrity",
    visibilityString = "PUBLIC",
    builderVisibilityString = "PUBLIC")
interface BaseValidateTableIntegrity extends ValidateTableIntegrity {

  @Value.Immutable
  interface Result extends ValidateTableIntegrity.Result {

    @Override
    @Value.Default
    default long totalMetadataFiles() {
      return 0L;
    }

    @Override
    @Value.Default
    default List<String> missingMetadataFiles() {
      return ImmutableList.of();
    }

    @Override
    @Value.Default
    default long totalDataFiles() {
      return 0L;
    }

    @Override
    @Value.Default
    default List<String> missingDataFiles() {
      return ImmutableList.of();
    }

    @Override
    @Value.Default
    default long totalDeleteFiles() {
      return 0L;
    }

    @Override
    @Value.Default
    default List<String> missingDeleteFiles() {
      return ImmutableList.of();
    }

    @Override
    @Value.Derived
    default boolean isValid() {
      return missingMetadataFiles().isEmpty()
          && missingDataFiles().isEmpty()
          && missingDeleteFiles().isEmpty();
    }

    @Override
    @Value.Derived
    default long missingFileCount() {
      return (long) missingMetadataFiles().size()
          + missingDataFiles().size()
          + missingDeleteFiles().size();
    }

    @Override
    @Value.Derived
    default String validationSummary() {
      return String.format(
          Locale.ROOT,
          "Validation Summary:%n"
              + "  Total Metadata Files Validated: %d%n"
              + "  Missing Metadata Files: %d%n"
              + "  Total Data Files Validated: %d%n"
              + "  Missing Data Files: %d%n"
              + "  Total Delete Files Validated: %d%n"
              + "  Missing Delete Files: %d%n"
              + "  Status: %s",
          totalMetadataFiles(),
          missingMetadataFiles().size(),
          totalDataFiles(),
          missingDataFiles().size(),
          totalDeleteFiles(),
          missingDeleteFiles().size(),
          isValid() ? "PASSED" : "FAILED");
    }
  }
}
