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

package org.apache.iceberg.spark;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;

public class SparkUtil {

  private SparkUtil() {
  }

  private static final Joiner DOT = Joiner.on(".");

  /**
   * Applies a list of Spark table changes to an {@link UpdateProperties} operation.
   * <p>
   * All non-property changes in the list are ignored.
   *
   * @param pendingUpdate an uncommitted UpdateProperties operation to configure
   * @param changes a list of Spark table changes
   * @return the UpdateProperties operation configured with the changes
   */
  public static UpdateProperties applyPropertyChanges(UpdateProperties pendingUpdate, List<TableChange> changes) {
    for (TableChange change : changes) {
      if (change instanceof TableChange.SetProperty) {
        TableChange.SetProperty set = (TableChange.SetProperty) change;
        pendingUpdate.set(set.property(), set.value());

      } else if (change instanceof TableChange.RemoveProperty) {
        TableChange.RemoveProperty remove = (TableChange.RemoveProperty) change;
        pendingUpdate.remove(remove.property());
      }
    }

    return pendingUpdate;
  }

  /**
   * Applies a list of Spark table changes to an {@link UpdateSchema} operation.
   * <p>
   * All non-schema changes in the list are ignored.
   *
   * @param pendingUpdate an uncommitted UpdateSchema operation to configure
   * @param changes a list of Spark table changes
   * @return the UpdateSchema operation configured with the changes
   */
  public static UpdateSchema applySchemaChanges(UpdateSchema pendingUpdate, List<TableChange> changes) {
    for (TableChange change : changes) {
      if (change instanceof TableChange.AddColumn) {
        TableChange.AddColumn add = (TableChange.AddColumn) change;
        Type type = SparkSchemaUtil.convert(add.dataType());
        pendingUpdate.addColumn(parentName(add.fieldNames()), leafName(add.fieldNames()), type, add.comment());

      } else if (change instanceof TableChange.UpdateColumnType) {
        TableChange.UpdateColumnType update = (TableChange.UpdateColumnType) change;
        Type newType = SparkSchemaUtil.convert(update.newDataType());
        Preconditions.checkArgument(newType.isPrimitiveType(),
            "Cannot update '%s', not a primitive type: %s", DOT.join(update.fieldNames()), update.newDataType());
        pendingUpdate.updateColumn(DOT.join(update.fieldNames()), newType.asPrimitiveType());

      } else if (change instanceof TableChange.UpdateColumnComment) {
        TableChange.UpdateColumnComment update = (TableChange.UpdateColumnComment) change;
        pendingUpdate.updateColumnDoc(DOT.join(update.fieldNames()), update.newComment());

      } else if (change instanceof TableChange.RenameColumn) {
        TableChange.RenameColumn rename = (TableChange.RenameColumn) change;
        pendingUpdate.renameColumn(DOT.join(rename.fieldNames()), rename.newName());

      } else if (change instanceof TableChange.DeleteColumn) {
        TableChange.DeleteColumn delete = (TableChange.DeleteColumn) change;
        pendingUpdate.deleteColumn(DOT.join(delete.fieldNames()));
      }
    }

    return pendingUpdate;
  }

  /**
   * Converts a PartitionSpec to Spark transforms.
   *
   * @param spec a PartitionSpec
   * @return an array of Transforms
   */
  public static Transform[] toTransforms(PartitionSpec spec) {
    List<Transform> transforms = PartitionSpecVisitor.visit(spec.schema(), spec,
        new PartitionSpecVisitor<Transform>() {
          @Override
          public Transform identity(String sourceName, int sourceId) {
            return Expressions.identity(sourceName);
          }

          @Override
          public Transform bucket(String sourceName, int sourceId, int width) {
            return Expressions.bucket(width, sourceName);
          }

          @Override
          public Transform truncate(String sourceName, int sourceId, int width) {
            return Expressions.apply("truncate", Expressions.column(sourceName), Expressions.literal(width));
          }

          @Override
          public Transform year(String sourceName, int sourceId) {
            return Expressions.years(sourceName);
          }

          @Override
          public Transform month(String sourceName, int sourceId) {
            return Expressions.months(sourceName);
          }

          @Override
          public Transform day(String sourceName, int sourceId) {
            return Expressions.days(sourceName);
          }

          @Override
          public Transform hour(String sourceName, int sourceId) {
            return Expressions.hours(sourceName);
          }
        });

    return transforms.toArray(new Transform[0]);
  }

  /**
   * Converts Spark transforms into a {@link PartitionSpec}.
   *
   * @param schema the table schema
   * @param partitioning Spark Transforms
   * @return a PartitionSpec
   */
  public static PartitionSpec toPartitionSpec(Schema schema, Transform[] partitioning) {
    if (partitioning == null || partitioning.length == 0) {
      return PartitionSpec.unpartitioned();
    }

    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    for (Transform transform : partitioning) {
      Preconditions.checkArgument(transform.references().length == 1,
          "Cannot convert transform with more than one column reference: %s", transform);
      String colName = DOT.join(transform.references()[0].fieldNames());
      switch (transform.name()) {
        case "identity":
          builder.identity(colName);
          break;
        case "bucket":
          builder.bucket(colName, findWidth(transform));
          break;
        case "years":
          builder.year(colName);
          break;
        case "months":
          builder.month(colName);
          break;
        case "days":
          builder.day(colName);
          break;
        case "hours":
          builder.hour(colName);
          break;
        case "truncate":
          builder.truncate(colName, findWidth(transform));
          break;
        default:
          throw new UnsupportedOperationException("Transform is not supported: " + transform);
      }
    }

    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private static int findWidth(Transform transform) {
    for (Expression expr : transform.arguments()) {
      if (expr instanceof Literal) {
        if (((Literal) expr).dataType() instanceof IntegerType) {
          Literal<Integer> lit = (Literal<Integer>) expr;
          Preconditions.checkArgument(lit.value() > 0,
              "Unsupported width for transform: %s", transform.describe());
          return lit.value();

        } else if (((Literal) expr).dataType() instanceof LongType) {
          Literal<Long> lit = (Literal<Long>) expr;
          Preconditions.checkArgument(lit.value() > 0 && lit.value() < Integer.MAX_VALUE,
              "Unsupported width for transform: %s", transform.describe());
          if (lit.value() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException();
          }
          return lit.value().intValue();
        }
      }
    }

    throw new IllegalArgumentException("Cannot find width for transform: " + transform.describe());
  }

  private static String leafName(String[] fieldNames) {
    Preconditions.checkArgument(fieldNames.length > 0, "Invalid field name: at least one name is required");
    return fieldNames[fieldNames.length - 1];
  }

  private static String parentName(String[] fieldNames) {
    if (fieldNames.length > 1) {
      return DOT.join(Arrays.copyOfRange(fieldNames, 0, fieldNames.length - 1));
    }
    return null;
  }
}
