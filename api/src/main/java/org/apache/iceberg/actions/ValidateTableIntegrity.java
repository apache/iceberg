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
import java.util.Map;
import org.apache.iceberg.Table;

/**
 * An action that validates the referential integrity of an Iceberg table's metadata — that every
 * metadata, data, and delete file the table's metadata references actually exists at its stated
 * location (or at a corresponding rewritten location when a destination is configured).
 *
 * <p>Supports two shapes:
 *
 * <ul>
 *   <li><b>Self-audit</b> — configure a single table with no destination. The action walks that
 *       table's metadata and verifies each referenced file exists at its stated location. Use this
 *       for standalone integrity checks: post-restore verification, catalog audits, and detecting
 *       storage-side drift (files removed out-of-band while metadata still references them).
 *   <li><b>Source-vs-destination</b> — configure a source and a destination together with a
 *       location prefix rewrite. The action walks source metadata, rewrites each referenced path to
 *       its destination, and verifies existence at the destination. Use this for pre-registration
 *       verification of a fresh copy, DR pre-promotion checks, migration audits, or any out-of-band
 *       copy (backups, Distcp output, manual file copies).
 * </ul>
 *
 * <p>The action walks the full metadata graph — metadata.json log entries, manifest lists,
 * manifests, data files, delete files (including V3 deletion vectors), statistics files, and
 * partition statistics — verifying every referenced file exists at the checked location.
 *
 * <p>Three modes control which files are validated, selected by which parameters are configured:
 *
 * <ul>
 *   <li><b>Backfill</b> validates every file the source references at {@link
 *       #sourceSnapshotId(long)}, scoped by {@link #validateScope(ValidateScope)}. Enabled when
 *       destination setters are not configured; this is also the self-audit path.
 *   <li><b>Incremental</b> validates only the files source has accumulated between {@link
 *       #destinationSnapshotId(long)} (exclusive) and {@link #sourceSnapshotId(long)} (inclusive).
 *       It does not re-check files that already existed at {@link #destinationSnapshotId(long)};
 *       those are presumed to have been validated when the destination was last at that snapshot.
 *   <li><b>Forced full</b>, enabled with {@link #validateFullTable(boolean)} set to {@code true},
 *       validates the full source file set against the destination regardless of any configured
 *       destination snapshot id.
 * </ul>
 *
 * <p>Use forced full validation when the destination state at {@link #destinationSnapshotId(long)}
 * cannot be presumed correct — for example, when no prior validation result is available, when
 * destination files may have been altered out-of-band, or when running an audit-from-scratch.
 *
 * <p>Self-audit example — verify a single table's metadata references files that still exist:
 *
 * <pre>{@code
 * Result result = SparkActions.get(spark)
 *     .validateTableIntegrity(table)
 *     .sourceMetadataVersion(version)
 *     .sourceSnapshotId(snapshotId)
 *     .execute();
 * }</pre>
 *
 * <p>Source-vs-destination example — verify a fresh copy before registering it:
 *
 * <pre>{@code
 * Result result = SparkActions.get(spark)
 *     .validateTableIntegrity(sourceTable)
 *     .sourceMetadataVersion(version)
 *     .sourceSnapshotId(snapshotId)
 *     .destinationTable(destinationTable)
 *     .destinationMetadataVersion(destinationVersion)
 *     .rewriteLocationPrefix(sourcePrefix, destinationPrefix)
 *     .execute();
 * }</pre>
 *
 * <p>Incremental source-vs-destination example — verify two tables stay in sync after an
 * incremental copy. Pass the destination's snapshot id as it was <i>before</i> the latest copy ran:
 *
 * <pre>{@code
 * Result result = SparkActions.get(spark)
 *     .validateTableIntegrity(sourceTable)
 *     .sourceMetadataVersion(currentSourceVersion)
 *     .sourceSnapshotId(currentSourceSnapshotId)
 *     .destinationTable(destinationTable)
 *     .destinationMetadataVersion(currentDestinationVersion)
 *     .destinationSnapshotId(preCopyDestinationSnapshotId)
 *     .rewriteLocationPrefix(sourcePrefix, destinationPrefix)
 *     .execute();
 * }</pre>
 */
public interface ValidateTableIntegrity
    extends Action<ValidateTableIntegrity, ValidateTableIntegrity.Result> {

  /**
   * Sets the destination table, enabling source-vs-destination validation. Must be paired with
   * {@link #destinationMetadataVersion}; both must be set together, or both must be left unset to
   * run as a self-audit on the source table.
   */
  ValidateTableIntegrity destinationTable(Table table);

  /** Sets the source metadata version (file name or absolute path). Required. */
  ValidateTableIntegrity sourceMetadataVersion(String version);

  /**
   * Sets the source snapshot id. Required unless the source metadata has no current snapshot (empty
   * source table); in that case the action validates only the metadata files.
   */
  ValidateTableIntegrity sourceSnapshotId(long snapshotId);

  /**
   * Sets the destination metadata version. Must be paired with {@link #destinationTable}; both must
   * be set together, or both must be left unset to run as a self-audit on the source table.
   */
  ValidateTableIntegrity destinationMetadataVersion(String version);

  /**
   * Sets the destination snapshot id used as the lower bound of the incremental diff. This is the
   * snapshot id the destination held at the time of the previous successful copy or validation.
   *
   * <p>When set, the action validates only files the source has accumulated between this snapshot
   * id (exclusive) and {@link #sourceSnapshotId(long)} (inclusive); files that existed at this
   * snapshot id are not re-checked. Callers that require validation of every source file regardless
   * of prior destination state should call {@link #validateFullTable(boolean)} with {@code true}.
   *
   * <p>Leave unset when no prior known-good state exists (first-time validation, initial copy, or
   * fresh audit): the action validates the full source file set at {@link #sourceSnapshotId(long)}
   * against the destination, without performing a diff.
   *
   * <p>After a workflow that preserves snapshot identity between source and destination (such as a
   * copy that uses {@link RewriteTablePath} to generate destination metadata), setting this to the
   * destination's current snapshot id produces an empty incremental diff (zero files validated).
   * Pass the previous snapshot id, or call {@link #validateFullTable(boolean)} with {@code true},
   * instead.
   *
   * <p>If this snapshot id is not present in the source's snapshot history at execution time — for
   * example, the source's retention policy has since expired it, or the destination committed
   * independently of the source — the action falls back to a full source-vs-destination diff and
   * logs a warning rather than throwing. The fallback result is semantically correct (any source
   * files missing at the destination are still reported), and the warning makes the condition
   * observable to callers that need to act on it.
   *
   * <p>Requires {@link #destinationTable(Table)} to be set.
   */
  ValidateTableIntegrity destinationSnapshotId(long snapshotId);

  /**
   * Forces backfill semantics even when destination parameters are set. Use this when the
   * destination has diverged from the source and the incremental diff would be incorrect, or when a
   * full audit of every expected file is required regardless of destination state.
   */
  ValidateTableIntegrity validateFullTable(boolean validateFull);

  /**
   * Sets the scope of content file validation. Affects backfill mode only — has no effect on
   * incremental validation, which always computes the source-vs-destination snapshot diff.
   */
  ValidateTableIntegrity validateScope(ValidateScope scope);

  /** Adds a source-to-destination location prefix rewrite rule. */
  ValidateTableIntegrity rewriteLocationPrefix(String sourcePrefix, String destinationPrefix);

  /**
   * Adds multiple source-to-destination location prefix rewrite rules. Equivalent to calling {@link
   * #rewriteLocationPrefix(String, String)} once per entry; entries accumulate with any
   * previously-added rules and are applied longest-prefix first at lookup time.
   */
  default ValidateTableIntegrity rewriteLocationPrefix(Map<String, String> prefixMap) {
    prefixMap.forEach(this::rewriteLocationPrefix);
    return this;
  }

  /**
   * Sets destination catalog properties used to resolve {@link org.apache.iceberg.io.FileIO} when
   * no destination table is configured. Useful for backfill validation against a destination that
   * is not yet registered as a loaded Iceberg table; the properties are passed to {@code
   * CatalogUtil.loadFileIO} together with any FileIO-specific configuration (region, endpoint,
   * credentials, etc.).
   */
  ValidateTableIntegrity destinationCatalogProperties(Map<String, String> catalogProperties);

  /** The action result. */
  interface Result {

    /** Returns true when no expected files are missing at the checked location. */
    boolean isValid();

    /**
     * Total metadata files validated. In incremental mode, counts only metadata files newly added
     * at the source since {@link ValidateTableIntegrity#destinationSnapshotId}. In backfill mode,
     * counts every metadata file (metadata.json entries, manifest lists, manifests, statistics,
     * partition statistics) reachable from the configured source snapshot.
     */
    long totalMetadataFiles();

    /** Paths of metadata files missing from the checked location. */
    List<String> missingMetadataFiles();

    /**
     * Total data files validated. In incremental mode, counts only data files newly added at the
     * source since {@link ValidateTableIntegrity#destinationSnapshotId}. In backfill mode, counts
     * every data file referenced by the source — at the source snapshot when {@link
     * ValidateScope#LATEST} is configured, across the entire source history when {@link
     * ValidateScope#ALL} is configured.
     */
    long totalDataFiles();

    /** Paths of data files missing from the checked location. */
    List<String> missingDataFiles();

    /**
     * Total delete files validated. Scoped the same way as {@link #totalDataFiles()}: incremental
     * mode counts only newly added delete files; backfill mode counts every delete file referenced
     * by the source within the configured scope.
     */
    long totalDeleteFiles();

    /** Paths of delete files missing from the checked location. */
    List<String> missingDeleteFiles();

    /** Combined count of all missing files. */
    long missingFileCount();

    /** Human-readable summary of the result. */
    String validationSummary();
  }

  /** Scope of content file validation in backfill mode. Has no effect on incremental validation. */
  enum ValidateScope {
    /** Validate content files referenced by every snapshot in the table history. */
    ALL,

    /** Validate content files referenced only by the latest snapshot. */
    LATEST;

    public static ValidateScope fromString(String value) {
      try {
        return ValidateScope.valueOf(value.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid validateTableIntegrity scope '%s'. Must be one of: all, latest", value));
      }
    }
  }
}
