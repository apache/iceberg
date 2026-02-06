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

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class ChangelogUtil {

  private static final Schema CHANGELOG_METADATA =
      new Schema(CHANGE_TYPE, CHANGE_ORDINAL, COMMIT_SNAPSHOT_ID);

  private static final Set<Integer> CHANGELOG_METADATA_FIELD_IDS =
      CHANGELOG_METADATA.columns().stream()
          .map(Types.NestedField::fieldId)
          .collect(Collectors.toSet());

  private ChangelogUtil() {}

  public static Schema changelogSchema(Schema tableSchema) {
    return TypeUtil.join(tableSchema, CHANGELOG_METADATA);
  }

  public static Schema dropChangelogMetadata(Schema changelogSchema) {
    return TypeUtil.selectNot(changelogSchema, CHANGELOG_METADATA_FIELD_IDS);
  }
}
