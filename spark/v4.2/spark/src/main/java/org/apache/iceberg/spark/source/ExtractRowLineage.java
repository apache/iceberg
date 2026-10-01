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

import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

class ExtractRowLineage implements Function<InternalRow, InternalRow> {
  private static final StructType ROW_LINEAGE_SCHEMA =
      new StructType()
          .add(MetadataColumns.ROW_ID.name(), LongType$.MODULE$, true)
          .add(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), LongType$.MODULE$, true);
  private static final InternalRow EMPTY_LINEAGE_ROW = new GenericInternalRow(2);

  private final boolean rowLineageRequired;

  private ProjectingInternalRow cachedRowLineageProjection;

  ExtractRowLineage(Schema writeSchema) {
    Preconditions.checkArgument(writeSchema != null, "Write schema cannot be null");
    this.rowLineageRequired = writeSchema.findField(MetadataColumns.ROW_ID.name()) != null;
  }

  @Override
  public InternalRow apply(InternalRow meta) {
    // If row lineage is not required on write return a null row
    if (!rowLineageRequired) {
      return null;
    }

    // If metadata row is null but the write schema requires lineage, return an empty lineage row
    if (meta == null) {
      return EMPTY_LINEAGE_ROW;
    }

    ProjectingInternalRow metaProj = (ProjectingInternalRow) meta;
    // Use cached ordinals if they exist
    if (cachedRowLineageProjection == null) {
      this.cachedRowLineageProjection = rowLineageProjection(metaProj);
    }

    cachedRowLineageProjection.project(metaProj);
    return cachedRowLineageProjection;
  }

  private ProjectingInternalRow rowLineageProjection(ProjectingInternalRow metadataRow) {
    Integer rowIdOrdinal = null;
    Integer lastUpdatedOrdinal = null;
    for (int i = 0; i < metadataRow.numFields(); i++) {
      String fieldName = metadataRow.schema().fields()[i].name();
      if (fieldName.equals(MetadataColumns.ROW_ID.name())) {
        rowIdOrdinal = i;
      } else if (fieldName.equals(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name())) {
        lastUpdatedOrdinal = i;
      }
    }

    Preconditions.checkArgument(rowIdOrdinal != null, "Expected to find row ID in metadata row");
    Preconditions.checkArgument(
        lastUpdatedOrdinal != null,
        "Expected to find last updated sequence number in metadata row");
    List<Object> rowLineageProjectionOrdinals = ImmutableList.of(rowIdOrdinal, lastUpdatedOrdinal);
    return new ProjectingInternalRow(
        ROW_LINEAGE_SCHEMA, JavaConverters.asScala(rowLineageProjectionOrdinals).toIndexedSeq());
  }
}
