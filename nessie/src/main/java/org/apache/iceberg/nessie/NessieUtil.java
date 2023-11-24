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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NessieUtil {

  private static final Logger LOG = LoggerFactory.getLogger(NessieUtil.class);

  public static final String NESSIE_CONFIG_PREFIX = "nessie.";
  static final String APPLICATION_TYPE = "application-type";

  public static final String CLIENT_API_VERSION = "nessie.client-api-version";

  private NessieUtil() {}

  static TableIdentifier removeCatalogName(TableIdentifier to, String name) {

    String[] levels = to.namespace().levels();
    // check if the identifier includes the catalog name and remove it
    if (levels.length >= 2 && name.equalsIgnoreCase(to.namespace().level(0))) {
      Namespace trimmedNamespace = Namespace.of(Arrays.copyOfRange(levels, 1, levels.length));
      return TableIdentifier.of(trimmedNamespace, to.name());
    }

    // return the original unmodified
    return to;
  }

  static ContentKey toKey(TableIdentifier tableIdentifier) {
    List<String> identifiers = Lists.newArrayList();
    if (tableIdentifier.hasNamespace()) {
      identifiers.addAll(Arrays.asList(tableIdentifier.namespace().levels()));
    }
    identifiers.add(tableIdentifier.name());

    return ContentKey.of(identifiers);
  }

  static CommitMeta buildCommitMetadata(String commitMsg, Map<String, String> catalogOptions) {
    return catalogOptions(CommitMeta.builder().message(commitMsg), catalogOptions).build();
  }

  static ImmutableCommitMeta.Builder catalogOptions(
      ImmutableCommitMeta.Builder commitMetaBuilder, Map<String, String> catalogOptions) {
    Preconditions.checkArgument(null != catalogOptions, "catalogOptions must not be null");
    commitMetaBuilder.author(NessieUtil.commitAuthor(catalogOptions));
    commitMetaBuilder.putProperties(APPLICATION_TYPE, "iceberg");
    if (catalogOptions.containsKey(CatalogProperties.APP_ID)) {
      commitMetaBuilder.putProperties(
          CatalogProperties.APP_ID, catalogOptions.get(CatalogProperties.APP_ID));
    }
    return commitMetaBuilder;
  }

  /**
   * @param catalogOptions The options where to look for the <b>user</b>
   * @return The author that can be used for a commit, which is either the <b>user</b> from the
   *     given <code>catalogOptions</code> or the logged in user as defined in the <b>user.name</b>
   *     JVM properties.
   */
  @Nullable
  private static String commitAuthor(Map<String, String> catalogOptions) {
    return Optional.ofNullable(catalogOptions.get(CatalogProperties.USER))
        .orElseGet(() -> System.getProperty("user.name"));
  }

  private static void checkAndUpdateGCProperties(
      TableMetadata tableMetadata, Map<String, String> updatedProperties, String identifier) {
    if (tableMetadata.propertyAsBoolean(
        NessieTableOperations.NESSIE_GC_NO_WARNING_PROPERTY, false)) {
      return;
    }

    // To prevent accidental deletion of files that are still referenced by other branches/tags,
    // setting GC_ENABLED to 'false' is recommended, so that all Iceberg's gc operations like
    // expire_snapshots, remove_orphan_files, drop_table with purge will fail with an error.
    // `nessie-gc` CLI provides a reference-aware GC functionality for the expired/unreferenced
    // files.
    // Advanced users may still want to use the simpler Iceberg GC tools iff their Nessie Server
    // contains only one branch (in which case the full Nessie history will be reflected in the
    // Iceberg sequence of snapshots).
    if (tableMetadata.propertyAsBoolean(
            TableProperties.GC_ENABLED, TableProperties.GC_ENABLED_DEFAULT)
        || tableMetadata.propertyAsBoolean(
            TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
            TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT)) {
      updatedProperties.put(NessieTableOperations.NESSIE_GC_NO_WARNING_PROPERTY, "true");
      LOG.warn(
          "The Iceberg property '{}' and/or '{}' is enabled on table '{}' in NessieCatalog."
              + " This will likely make data in other Nessie branches and tags and in earlier, historical Nessie"
              + " commits inaccessible. The recommended setting for those properties is 'false'. Use the 'nessie-gc'"
              + " tool for Nessie reference-aware garbage collection.",
          TableProperties.GC_ENABLED,
          TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
          identifier);
    }
  }

  public static TableMetadata updateTableMetadataWithNessieSpecificProperties(
      TableMetadata tableMetadata,
      String metadataLocation,
      IcebergTable table,
      String identifier,
      Reference reference) {
    // Update the TableMetadata with the Content of NessieTableState.
    Map<String, String> newProperties = Maps.newHashMap(tableMetadata.properties());
    newProperties.put(NessieTableOperations.NESSIE_COMMIT_ID_PROPERTY, reference.getHash());

    checkAndUpdateGCProperties(tableMetadata, newProperties, identifier);

    TableMetadata.Builder builder =
        TableMetadata.buildFrom(tableMetadata)
            .setPreviousFileLocation(null)
            .setCurrentSchema(table.getSchemaId())
            .setDefaultSortOrder(table.getSortOrderId())
            .setDefaultPartitionSpec(table.getSpecId())
            .withMetadataLocation(metadataLocation)
            .setProperties(newProperties);
    if (table.getSnapshotId() != -1) {
      builder.setBranchSnapshot(table.getSnapshotId(), SnapshotRef.MAIN_BRANCH);
    }
    LOG.info(
        "loadTableMetadata for '{}' from location '{}' at '{}'",
        identifier,
        metadataLocation,
        reference);

    return builder.discardChanges().build();
  }

  public static Optional<Conflict> extractSingleConflict(
      NessieReferenceConflictException ex, Collection<Conflict.ConflictType> handledConflictTypes) {
    // Check if the server returned 'ReferenceConflicts' information
    ReferenceConflicts referenceConflicts = ex.getErrorDetails();
    if (referenceConflicts == null) {
      return Optional.empty();
    }

    List<Conflict> conflicts =
        referenceConflicts.conflicts().stream()
            .filter(c -> handledConflictTypes.contains(c.conflictType()))
            .collect(Collectors.toList());
    if (conflicts.size() != 1) {
      return Optional.empty();
    }

    Conflict conflict = conflicts.get(0);
    return Optional.of(conflict);
  }
}
