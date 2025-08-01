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
package org.apache.iceberg.flink.util;

import java.util.List;
import java.util.Map;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;

public class FlinkAlterTableUtil {
  private FlinkAlterTableUtil() {}

  public static void commitChanges(
      Table table,
      String setLocation,
      String setSnapshotId,
      String pickSnapshotId,
      Map<String, String> setProperties) {
    commitManageSnapshots(table, setSnapshotId, pickSnapshotId);

    Transaction transaction = table.newTransaction();

    if (setLocation != null) {
      transaction.updateLocation().setLocation(setLocation).commit();
    }

    if (!setProperties.isEmpty()) {
      UpdateProperties updateProperties = transaction.updateProperties();
      setProperties.forEach(
          (k, v) -> {
            if (v == null) {
              updateProperties.remove(k);
            } else {
              updateProperties.set(k, v);
            }
          });
      updateProperties.commit();
    }

    transaction.commitTransaction();
  }

  public static void commitChanges(
      Table table,
      String setLocation,
      String setSnapshotId,
      String pickSnapshotId,
      List<TableChange> schemaChanges,
      List<TableChange> propertyChanges) {
    commitManageSnapshots(table, setSnapshotId, pickSnapshotId);

    Transaction transaction = table.newTransaction();

    if (setLocation != null) {
      transaction.updateLocation().setLocation(setLocation).commit();
    }

    if (!schemaChanges.isEmpty()) {
      UpdateSchema updateSchema = transaction.updateSchema();
      FlinkAlterTableUtil.applySchemaChanges(updateSchema, schemaChanges);
      updateSchema.commit();
    }

    if (!propertyChanges.isEmpty()) {
      UpdateProperties updateProperties = transaction.updateProperties();
      FlinkAlterTableUtil.applyPropertyChanges(updateProperties, propertyChanges);
      updateProperties.commit();
    }

    transaction.commitTransaction();
  }

  public static void commitManageSnapshots(
      Table table, String setSnapshotId, String cherrypickSnapshotId) {
    // don't allow setting the snapshot and picking a commit at the same time because order is
    // ambiguous and choosing one order leads to different results
    Preconditions.checkArgument(
        setSnapshotId == null || cherrypickSnapshotId == null,
        "Cannot set the current snapshot ID and cherry-pick snapshot changes");

    if (setSnapshotId != null) {
      long newSnapshotId = Long.parseLong(setSnapshotId);
      table.manageSnapshots().setCurrentSnapshot(newSnapshotId).commit();
    }

    // if updating the table snapshot, perform that update first in case it fails
    if (cherrypickSnapshotId != null) {
      long newSnapshotId = Long.parseLong(cherrypickSnapshotId);
      table.manageSnapshots().cherrypick(newSnapshotId).commit();
    }
  }

  /**
   * Applies a list of Flink table changes to an {@link UpdateSchema} operation.
   *
   * @param pendingUpdate an uncommitted UpdateSchema operation to configure
   * @param schemaChanges a list of Flink table changes
   */
  public static void applySchemaChanges(
      UpdateSchema pendingUpdate, List<TableChange> schemaChanges) {
    for (TableChange change : schemaChanges) {
      if (change instanceof TableChange.AddColumn) {
        TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
        Column flinkColumn = addColumn.getColumn();
        Preconditions.checkArgument(
            FlinkCompatibilityUtil.isPhysicalColumn(flinkColumn),
            "Unsupported table change: Adding computed column %s.",
            flinkColumn.getName());
        Type icebergType = FlinkSchemaUtil.convert(flinkColumn.getDataType().getLogicalType());
        if (flinkColumn.getDataType().getLogicalType().isNullable()) {
          pendingUpdate.addColumn(
              flinkColumn.getName(), icebergType, flinkColumn.getComment().orElse(null));
        } else {
          pendingUpdate.addRequiredColumn(
              flinkColumn.getName(), icebergType, flinkColumn.getComment().orElse(null));
        }
      } else if (change instanceof TableChange.ModifyColumn) {
        TableChange.ModifyColumn modifyColumn = (TableChange.ModifyColumn) change;
        applyModifyColumn(pendingUpdate, modifyColumn);
      } else if (change instanceof TableChange.DropColumn) {
        TableChange.DropColumn dropColumn = (TableChange.DropColumn) change;
        pendingUpdate.deleteColumn(dropColumn.getColumnName());
      } else if (change instanceof TableChange.AddWatermark) {
        throw new UnsupportedOperationException("Unsupported table change: AddWatermark.");
      } else if (change instanceof TableChange.ModifyWatermark) {
        throw new UnsupportedOperationException("Unsupported table change: ModifyWatermark.");
      } else if (change instanceof TableChange.DropWatermark) {
        throw new UnsupportedOperationException("Unsupported table change: DropWatermark.");
      } else if (change instanceof TableChange.AddUniqueConstraint) {
        TableChange.AddUniqueConstraint addPk = (TableChange.AddUniqueConstraint) change;
        applyUniqueConstraint(pendingUpdate, addPk.getConstraint());
      } else if (change instanceof TableChange.ModifyUniqueConstraint) {
        TableChange.ModifyUniqueConstraint modifyPk = (TableChange.ModifyUniqueConstraint) change;
        applyUniqueConstraint(pendingUpdate, modifyPk.getNewConstraint());
      } else if (change instanceof TableChange.DropConstraint) {
        throw new UnsupportedOperationException("Unsupported table change: DropConstraint.");
      } else {
        throw new UnsupportedOperationException("Cannot apply unknown table change: " + change);
      }
    }
  }

  /**
   * Applies a list of Flink table property changes to an {@link UpdateProperties} operation.
   *
   * @param pendingUpdate an uncommitted UpdateProperty operation to configure
   * @param propertyChanges a list of Flink table changes
   */
  public static void applyPropertyChanges(
      UpdateProperties pendingUpdate, List<TableChange> propertyChanges) {
    for (TableChange change : propertyChanges) {
      if (change instanceof TableChange.SetOption) {
        TableChange.SetOption setOption = (TableChange.SetOption) change;
        pendingUpdate.set(setOption.getKey(), setOption.getValue());
      } else if (change instanceof TableChange.ResetOption) {
        TableChange.ResetOption resetOption = (TableChange.ResetOption) change;
        pendingUpdate.remove(resetOption.getKey());
      } else {
        throw new UnsupportedOperationException(
            "The given table change is not a property change: " + change);
      }
    }
  }

  private static void applyModifyColumn(
      UpdateSchema pendingUpdate, TableChange.ModifyColumn modifyColumn) {
    if (modifyColumn instanceof TableChange.ModifyColumnName) {
      TableChange.ModifyColumnName modifyName = (TableChange.ModifyColumnName) modifyColumn;
      pendingUpdate.renameColumn(modifyName.getOldColumnName(), modifyName.getNewColumnName());
    } else if (modifyColumn instanceof TableChange.ModifyColumnPosition) {
      TableChange.ModifyColumnPosition modifyPosition =
          (TableChange.ModifyColumnPosition) modifyColumn;
      applyModifyColumnPosition(pendingUpdate, modifyPosition);
    } else if (modifyColumn instanceof TableChange.ModifyPhysicalColumnType) {
      TableChange.ModifyPhysicalColumnType modifyType =
          (TableChange.ModifyPhysicalColumnType) modifyColumn;
      Type type = FlinkSchemaUtil.convert(modifyType.getNewType().getLogicalType());
      String columnName = modifyType.getOldColumn().getName();
      pendingUpdate.updateColumn(columnName, type.asPrimitiveType());
      if (modifyType.getNewColumn().getDataType().getLogicalType().isNullable()) {
        pendingUpdate.makeColumnOptional(columnName);
      } else {
        pendingUpdate.requireColumn(columnName);
      }
    } else if (modifyColumn instanceof TableChange.ModifyColumnComment) {
      TableChange.ModifyColumnComment modifyComment =
          (TableChange.ModifyColumnComment) modifyColumn;
      pendingUpdate.updateColumnDoc(
          modifyComment.getOldColumn().getName(), modifyComment.getNewComment());
    } else {
      throw new UnsupportedOperationException(
          "Cannot apply unknown modify-column change: " + modifyColumn);
    }
  }

  private static void applyModifyColumnPosition(
      UpdateSchema pendingUpdate, TableChange.ModifyColumnPosition modifyColumnPosition) {
    TableChange.ColumnPosition newPosition = modifyColumnPosition.getNewPosition();
    if (newPosition instanceof TableChange.First) {
      pendingUpdate.moveFirst(modifyColumnPosition.getOldColumn().getName());
    } else if (newPosition instanceof TableChange.After) {
      TableChange.After after = (TableChange.After) newPosition;
      pendingUpdate.moveAfter(modifyColumnPosition.getOldColumn().getName(), after.column());
    } else {
      throw new UnsupportedOperationException(
          "Cannot apply unknown modify-column-position change: " + modifyColumnPosition);
    }
  }

  private static void applyUniqueConstraint(
      UpdateSchema pendingUpdate, UniqueConstraint constraint) {
    switch (constraint.getType()) {
      case PRIMARY_KEY:
        pendingUpdate.setIdentifierFields(constraint.getColumns());
        break;
      case UNIQUE_KEY:
        throw new UnsupportedOperationException(
            "Unsupported table change: setting unique key constraints.");
      default:
        throw new UnsupportedOperationException(
            "Cannot apply unknown unique constraint: " + constraint.getType().name());
    }
  }
}
