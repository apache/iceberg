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
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ChangelogUtil;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.connector.catalog.Changelog;
import org.apache.spark.unsafe.types.UTF8String;
import scala.jdk.javaapi.CollectionConverters;

abstract class ChangelogRowAdapter {

  private final Schema dataSchema;

  private ChangelogRowAdapter(Schema dataSchema) {
    this.dataSchema = dataSchema;
  }

  static ChangelogRowAdapter create(Table table, Schema expectedSchema) {
    return expectedSchema.findField(SparkChangelog.COMMIT_VERSION_ID) != null
        ? new SparkRowAdapter(table, expectedSchema)
        : new IcebergRowAdapter(expectedSchema);
  }

  Schema dataSchema() {
    return dataSchema;
  }

  abstract Function<InternalRow, InternalRow> rowMapper(ChangelogScanTask task);

  private static class IcebergRowAdapter extends ChangelogRowAdapter {

    private IcebergRowAdapter(Schema expectedSchema) {
      super(ChangelogUtil.dropChangelogMetadata(expectedSchema));
    }

    @Override
    Function<InternalRow, InternalRow> rowMapper(ChangelogScanTask task) {
      InternalRow metadata =
          new GenericInternalRow(
              new Object[] {
                UTF8String.fromString(task.operation().name()),
                task.changeOrdinal(),
                task.commitSnapshotId()
              });
      JoinedRow row = new JoinedRow();
      row.withRight(metadata);
      return row::withLeft;
    }
  }

  private static class SparkRowAdapter extends ChangelogRowAdapter {

    private final Table table;
    private final UnsafeProjection projection;

    private SparkRowAdapter(Table table, Schema expectedSchema) {
      super(SparkChangelog.dropCdcMetadata(expectedSchema));
      this.table = table;
      List<Integer> fieldIds = Lists.newArrayList();
      dataSchema().columns().forEach(field -> fieldIds.add(field.fieldId()));
      fieldIds.addAll(
          List.of(
              MetadataColumns.CHANGE_TYPE.fieldId(),
              SparkChangelog.COMMIT_VERSION_ID,
              SparkChangelog.COMMIT_TIMESTAMP_ID));
      List<Expression> expressions = Lists.newArrayList();
      for (Types.NestedField field : expectedSchema.columns()) {
        expressions.add(
            new BoundReference(
                fieldIds.indexOf(field.fieldId()),
                SparkSchemaUtil.convert(field.type()),
                field.isOptional()));
      }
      this.projection = UnsafeProjection.create(CollectionConverters.asScala(expressions).toSeq());
    }

    @Override
    Function<InternalRow, InternalRow> rowMapper(ChangelogScanTask task) {
      Snapshot snapshot = table.snapshot(task.commitSnapshotId());
      Preconditions.checkNotNull(
          snapshot, "Cannot find snapshot for changelog task: %s", task.commitSnapshotId());
      InternalRow metadata =
          new GenericInternalRow(
              new Object[] {
                UTF8String.fromString(changeType(task)),
                snapshot.sequenceNumber(),
                snapshot.timestampMillis() * 1000
              });
      JoinedRow row = new JoinedRow();
      row.withRight(metadata);
      return data -> projection.apply(row.withLeft(data));
    }

    private static String changeType(ChangelogScanTask task) {
      return switch (task.operation()) {
        case INSERT -> Changelog.CHANGE_TYPE_INSERT;
        case DELETE -> Changelog.CHANGE_TYPE_DELETE;
        case UPDATE_BEFORE -> Changelog.CHANGE_TYPE_UPDATE_PREIMAGE;
        case UPDATE_AFTER -> Changelog.CHANGE_TYPE_UPDATE_POSTIMAGE;
      };
    }
  }
}
