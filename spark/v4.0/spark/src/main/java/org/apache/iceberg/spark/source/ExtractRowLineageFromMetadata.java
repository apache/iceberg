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
package org.apache.iceberg.spark.source;

import java.util.function.Function;
import org.apache.iceberg.MetadataColumns;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

class ExtractRowLineageFromMetadata implements Function<InternalRow, InternalRow> {
  private Integer rowIdOrdinal;
  private Integer lastUpdatedOrdinal;

  @Override
  public InternalRow apply(InternalRow meta) {
    GenericInternalRow row = new GenericInternalRow(2);
    row.setNullAt(0);
    row.setNullAt(1);

    if (meta == null) {
      return row;
    }

    // Ordinals are cached
    if (rowIdOrdinal != null && lastUpdatedOrdinal != null) {
      setIfNotNull(row, 0, meta, rowIdOrdinal);
      setIfNotNull(row, 1, meta, lastUpdatedOrdinal);
      return row;
    }

    // Otherwise, discover ordinals and set values
    ProjectingInternalRow metaProj = (ProjectingInternalRow) meta;
    for (int i = 0; i < metaProj.numFields(); i++) {
      String fieldName = metaProj.schema().fields()[i].name();

      if (fieldName.equals(MetadataColumns.ROW_ID.name())) {
        rowIdOrdinal = i;
        setIfNotNull(row, 0, meta, i);
      } else if (fieldName.equals(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name())) {
        lastUpdatedOrdinal = i;
        setIfNotNull(row, 1, meta, i);
      }
    }
    return row;
  }

  private void setIfNotNull(GenericInternalRow row, int rowPos, InternalRow meta, int metaPos) {
    if (!meta.isNullAt(metaPos)) {
      row.setLong(rowPos, meta.getLong(metaPos));
    }
  }
}
