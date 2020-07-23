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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.sparkproject.guava.collect.ImmutableMap;

public class Spark3Util {

  private static final Set<String> LOCALITY_WHITELIST_FS = ImmutableSet.of("hdfs");
  private static final Set<String> RESERVED_PROPERTIES = ImmutableSet.of(
      TableCatalog.PROP_LOCATION, TableCatalog.PROP_PROVIDER);
  private static final Joiner DOT = Joiner.on(".");

  private Spark3Util() {
  }

  public static Map<String, String> rebuildCreateProperties(Map<String, String> createProperties) {
    ImmutableMap.Builder<String, String> tableProperties = ImmutableMap.builder();
    createProperties.entrySet().stream()
        .filter(entry -> !RESERVED_PROPERTIES.contains(entry.getKey()))
        .forEach(tableProperties::put);

    String provider = createProperties.get(TableCatalog.PROP_PROVIDER);
    if ("parquet".equalsIgnoreCase(provider)) {
      tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
    } else if ("avro".equalsIgnoreCase(provider)) {
      tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
    } else if ("orc".equalsIgnoreCase(provider)) {
      tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "orc");
    } else if (provider != null && !"iceberg".equalsIgnoreCase(provider)) {
      throw new IllegalArgumentException("Unsupported format in USING: " + provider);
    }

    return tableProperties.build();
  }

  /**
   * Applies a list of Spark table changes to an {@link UpdateProperties} operation.
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

      } else {
        throw new UnsupportedOperationException("Cannot apply unknown table change: " + change);
      }
    }

    return pendingUpdate;
  }

  /**
   * Applies a list of Spark table changes to an {@link UpdateSchema} operation.
   *
   * @param pendingUpdate an uncommitted UpdateSchema operation to configure
   * @param changes a list of Spark table changes
   * @return the UpdateSchema operation configured with the changes
   */
  public static UpdateSchema applySchemaChanges(UpdateSchema pendingUpdate, List<TableChange> changes) {
    for (TableChange change : changes) {
      if (change instanceof TableChange.AddColumn) {
        apply(pendingUpdate, (TableChange.AddColumn) change);

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

      } else if (change instanceof TableChange.UpdateColumnNullability) {
        TableChange.UpdateColumnNullability update = (TableChange.UpdateColumnNullability) change;
        if (update.nullable()) {
          pendingUpdate.makeColumnOptional(DOT.join(update.fieldNames()));
        } else {
          pendingUpdate.requireColumn(DOT.join(update.fieldNames()));
        }

      } else if (change instanceof TableChange.UpdateColumnPosition) {
        apply(pendingUpdate, (TableChange.UpdateColumnPosition) change);

      } else {
        throw new UnsupportedOperationException("Cannot apply unknown table change: " + change);
      }
    }

    return pendingUpdate;
  }

  private static void apply(UpdateSchema pendingUpdate, TableChange.UpdateColumnPosition update) {
    Preconditions.checkArgument(update.position() != null, "Invalid position: null");

    if (update.position() instanceof TableChange.After) {
      TableChange.After after = (TableChange.After) update.position();
      String referenceField = peerName(update.fieldNames(), after.column());
      pendingUpdate.moveAfter(DOT.join(update.fieldNames()), referenceField);

    } else if (update.position() instanceof TableChange.First) {
      pendingUpdate.moveFirst(DOT.join(update.fieldNames()));

    } else {
      throw new IllegalArgumentException("Unknown position for reorder: " + update.position());
    }
  }

  private static void apply(UpdateSchema pendingUpdate, TableChange.AddColumn add) {
    Type type = SparkSchemaUtil.convert(add.dataType());
    pendingUpdate.addColumn(parentName(add.fieldNames()), leafName(add.fieldNames()), type, add.comment());

    if (add.position() instanceof TableChange.After) {
      TableChange.After after = (TableChange.After) add.position();
      String referenceField = peerName(add.fieldNames(), after.column());
      pendingUpdate.moveAfter(DOT.join(add.fieldNames()), referenceField);

    } else if (add.position() instanceof TableChange.First) {
      pendingUpdate.moveFirst(DOT.join(add.fieldNames()));

    } else {
      Preconditions.checkArgument(add.position() == null,
          "Cannot add '%s' at unknown position: %s", DOT.join(add.fieldNames()), add.position());
    }
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
        case "date":
        case "days":
          builder.day(colName);
          break;
        case "date_hour":
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

  private static String peerName(String[] fieldNames, String fieldName) {
    if (fieldNames.length > 1) {
      String[] peerNames = Arrays.copyOf(fieldNames, fieldNames.length);
      peerNames[fieldNames.length - 1] = fieldName;
      return DOT.join(peerNames);
    }
    return fieldName;
  }

  private static String parentName(String[] fieldNames) {
    if (fieldNames.length > 1) {
      return DOT.join(Arrays.copyOfRange(fieldNames, 0, fieldNames.length - 1));
    }
    return null;
  }

  public static String describe(org.apache.iceberg.expressions.Expression expr) {
    return ExpressionVisitors.visit(expr, DescribeExpressionVisitor.INSTANCE);
  }

  public static String describe(Schema schema) {
    return TypeUtil.visit(schema, DescribeSchemaVisitor.INSTANCE);
  }

  public static String describe(Type type) {
    return TypeUtil.visit(type, DescribeSchemaVisitor.INSTANCE);
  }

  public static boolean isLocalityEnabled(FileIO io, String location, CaseInsensitiveStringMap readOptions) {
    InputFile in = io.newInputFile(location);
    if (in instanceof HadoopInputFile) {
      String scheme = ((HadoopInputFile) in).getFileSystem().getScheme();
      return readOptions.getBoolean("locality", LOCALITY_WHITELIST_FS.contains(scheme));
    }
    return false;
  }

  public static boolean isVectorizationEnabled(Map<String, String> properties, CaseInsensitiveStringMap readOptions) {
    return readOptions.getBoolean("vectorization-enabled",
        PropertyUtil.propertyAsBoolean(properties,
            TableProperties.PARQUET_VECTORIZATION_ENABLED, TableProperties.PARQUET_VECTORIZATION_ENABLED_DEFAULT));
  }

  public static int batchSize(Map<String, String> properties, CaseInsensitiveStringMap readOptions) {
    return readOptions.getInt("batch-size",
        PropertyUtil.propertyAsInt(properties,
            TableProperties.PARQUET_BATCH_SIZE, TableProperties.PARQUET_BATCH_SIZE_DEFAULT));
  }

  public static Long propertyAsLong(CaseInsensitiveStringMap options, String property, Long defaultValue) {
    if (defaultValue != null) {
      return options.getLong(property, defaultValue);
    }

    String value = options.get(property);
    if (value != null) {
      return Long.parseLong(value);
    }

    return null;
  }

  public static Integer propertyAsInt(CaseInsensitiveStringMap options, String property, Integer defaultValue) {
    if (defaultValue != null) {
      return options.getInt(property, defaultValue);
    }

    String value = options.get(property);
    if (value != null) {
      return Integer.parseInt(value);
    }

    return null;
  }

  public static class DescribeSchemaVisitor extends TypeUtil.SchemaVisitor<String> {
    private static final Joiner COMMA = Joiner.on(',');
    private static final DescribeSchemaVisitor INSTANCE = new DescribeSchemaVisitor();

    private DescribeSchemaVisitor() {
    }

    @Override
    public String schema(Schema schema, String structResult) {
      return structResult;
    }

    @Override
    public String struct(Types.StructType struct, List<String> fieldResults) {
      return "struct<" + COMMA.join(fieldResults) + ">";
    }

    @Override
    public String field(Types.NestedField field, String fieldResult) {
      return field.name() + ": " + fieldResult + (field.isRequired() ? " not null" : "");
    }

    @Override
    public String list(Types.ListType list, String elementResult) {
      return "map<" + elementResult + ">";
    }

    @Override
    public String map(Types.MapType map, String keyResult, String valueResult) {
      return "map<" + keyResult + ", " + valueResult + ">";
    }

    @Override
    public String primitive(Type.PrimitiveType primitive) {
      switch (primitive.typeId()) {
        case BOOLEAN:
          return "boolean";
        case INTEGER:
          return "int";
        case LONG:
          return "bigint";
        case FLOAT:
          return "float";
        case DOUBLE:
          return "double";
        case DATE:
          return "date";
        case TIME:
          return "time";
        case TIMESTAMP:
          return "timestamp";
        case STRING:
        case UUID:
          return "string";
        case FIXED:
        case BINARY:
          return "binary";
        case DECIMAL:
          Types.DecimalType decimal = (Types.DecimalType) primitive;
          return "decimal(" + decimal.precision() + "," + decimal.scale() + ")";
      }
      throw new UnsupportedOperationException("Cannot convert type to SQL: " + primitive);
    }
  }

  private static class DescribeExpressionVisitor extends ExpressionVisitors.ExpressionVisitor<String> {
    private static final DescribeExpressionVisitor INSTANCE = new DescribeExpressionVisitor();

    private DescribeExpressionVisitor() {
    }

    @Override
    public String alwaysTrue() {
      return "true";
    }

    @Override
    public String alwaysFalse() {
      return "false";
    }

    @Override
    public String not(String result) {
      return "NOT (" + result + ")";
    }

    @Override
    public String and(String leftResult, String rightResult) {
      return "(" + leftResult + " AND " + rightResult + ")";
    }

    @Override
    public String or(String leftResult, String rightResult) {
      return "(" + leftResult + " OR " + rightResult + ")";
    }

    @Override
    public <T> String predicate(BoundPredicate<T> pred) {
      throw new UnsupportedOperationException("Cannot convert bound predicates to SQL");
    }

    @Override
    public <T> String predicate(UnboundPredicate<T> pred) {
      switch (pred.op()) {
        case IS_NULL:
          return pred.ref().name() + " IS NULL";
        case NOT_NULL:
          return pred.ref().name() + " IS NOT NULL";
        case LT:
          return pred.ref().name() + " < " + sqlString(pred.literal());
        case LT_EQ:
          return pred.ref().name() + " <= " + sqlString(pred.literal());
        case GT:
          return pred.ref().name() + " > " + sqlString(pred.literal());
        case GT_EQ:
          return pred.ref().name() + " >= " + sqlString(pred.literal());
        case EQ:
          return pred.ref().name() + " = " + sqlString(pred.literal());
        case NOT_EQ:
          return pred.ref().name() + " != " + sqlString(pred.literal());
        case STARTS_WITH:
          return pred.ref().name() + " LIKE '" + pred.literal() + "%'";
        default:
          throw new UnsupportedOperationException("Cannot convert predicate to SQL: " + pred);
      }
    }

    private static String sqlString(org.apache.iceberg.expressions.Literal<?> lit) {
      if (lit.value() instanceof String) {
        return "'" + lit.value() + "'";
      } else if (lit.value() instanceof ByteBuffer) {
        throw new IllegalArgumentException("Cannot convert bytes to SQL literal: " + lit);
      } else {
        return lit.value().toString();
      }
    }
  }
}
