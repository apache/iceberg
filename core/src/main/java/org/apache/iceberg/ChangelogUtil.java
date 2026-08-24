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

import static org.apache.iceberg.MetadataColumns.CHANGE_ORDINAL;
import static org.apache.iceberg.MetadataColumns.CHANGE_TYPE;
import static org.apache.iceberg.MetadataColumns.COMMIT_SNAPSHOT_ID;
import static org.apache.iceberg.MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER;
import static org.apache.iceberg.MetadataColumns.ROW_ID;

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class ChangelogUtil {

  private static final Schema CHANGELOG_METADATA =
      new Schema(CHANGE_TYPE, CHANGE_ORDINAL, COMMIT_SNAPSHOT_ID);

  // ROW_ID and LAST_UPDATED_SEQUENCE_NUMBER are placed BEFORE the three constant columns so that
  // the JoinedRow column ordering is consistent with this schema:
  //   BaseReader emits [data..., _row_id, _last_updated_sequence_number]  (left side)
  //   changelogMetadata emits [_change_type, _change_ordinal, _commit_snapshot_id]  (right side)
  // giving final row order [data..., _row_id, _last_updated_seq, _change_type, _change_ordinal,
  // _commit_snapshot_id] which matches this schema column order.
  private static final Schema CHANGELOG_METADATA_WITH_ROW_LINEAGE =
      new Schema(
          ROW_ID,
          LAST_UPDATED_SEQUENCE_NUMBER,
          CHANGE_TYPE,
          CHANGE_ORDINAL,
          COMMIT_SNAPSHOT_ID);

  // Only the three constant-per-task columns are stripped by dropChangelogMetadata().
  // ROW_ID and LAST_UPDATED_SEQUENCE_NUMBER are computed per-row by the file reader
  // (via PartitionUtil.constantsMap + ParquetValueReaders.rowIds) so they must NOT
  // be removed from the schema passed to BaseReader.
  private static final Set<Integer> CHANGELOG_METADATA_FIELD_IDS =
      CHANGELOG_METADATA.columns().stream()
          .map(Types.NestedField::fieldId)
          .collect(Collectors.toSet());

  private ChangelogUtil() {}

  public static Schema changelogSchema(Schema tableSchema) {
    return TypeUtil.join(tableSchema, CHANGELOG_METADATA);
  }

  /**
   * Returns the changelog schema for the given table schema and format version. For format version
   * 3 and above, the schema includes {@code _row_id} and {@code _last_updated_sequence_number} in
   * addition to the standard changelog metadata columns.
   */
  public static Schema changelogSchema(Schema tableSchema, int formatVersion) {
    if (formatVersion >= 3) {
      return TypeUtil.join(tableSchema, CHANGELOG_METADATA_WITH_ROW_LINEAGE);
    }

    return TypeUtil.join(tableSchema, CHANGELOG_METADATA);
  }

  public static Schema dropChangelogMetadata(Schema changelogSchema) {
    return TypeUtil.selectNot(changelogSchema, CHANGELOG_METADATA_FIELD_IDS);
  }
}
