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

import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_NONE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_RANGE;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTableUtil.SparkPartition;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.transforms.SortOrderVisitor;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.SortOrderUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.iceberg.distributions.Distribution;
import org.apache.spark.sql.connector.iceberg.distributions.Distributions;
import org.apache.spark.sql.connector.iceberg.distributions.OrderedDistribution;
import org.apache.spark.sql.connector.iceberg.expressions.SortOrder;
import org.apache.spark.sql.execution.datasources.FileStatusCache;
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Predef;
import scala.Some;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class Spark3Util {

  private static final Set<String> RESERVED_PROPERTIES =
      ImmutableSet.of(TableCatalog.PROP_LOCATION, TableCatalog.PROP_PROVIDER);
  private static final Joiner DOT = Joiner.on(".");

  private Spark3Util() {}

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
  public static UpdateProperties applyPropertyChanges(
      UpdateProperties pendingUpdate, List<TableChange> changes) {
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
  public static UpdateSchema applySchemaChanges(
      UpdateSchema pendingUpdate, List<TableChange> changes) {
    for (TableChange change : changes) {
      if (change instanceof TableChange.AddColumn) {
        apply(pendingUpdate, (TableChange.AddColumn) change);

      } else if (change instanceof TableChange.UpdateColumnType) {
        TableChange.UpdateColumnType update = (TableChange.UpdateColumnType) change;
        Type newType = SparkSchemaUtil.convert(update.newDataType());
        Preconditions.checkArgument(
            newType.isPrimitiveType(),
            "Cannot update '%s', not a primitive type: %s",
            DOT.join(update.fieldNames()),
            update.newDataType());
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
    Preconditions.checkArgument(
        add.isNullable(),
        "Incompatible change: cannot add required column: %s",
        leafName(add.fieldNames()));
    Type type = SparkSchemaUtil.convert(add.dataType());
    pendingUpdate.addColumn(
        parentName(add.fieldNames()), leafName(add.fieldNames()), type, add.comment());

    if (add.position() instanceof TableChange.After) {
      TableChange.After after = (TableChange.After) add.position();
      String referenceField = peerName(add.fieldNames(), after.column());
      pendingUpdate.moveAfter(DOT.join(add.fieldNames()), referenceField);

    } else if (add.position() instanceof TableChange.First) {
      pendingUpdate.moveFirst(DOT.join(add.fieldNames()));

    } else {
      Preconditions.checkArgument(
          add.position() == null,
          "Cannot add '%s' at unknown position: %s",
          DOT.join(add.fieldNames()),
          add.position());
    }
  }

  public static org.apache.iceberg.Table toIcebergTable(Table table) {
    Preconditions.checkArgument(
        table instanceof SparkTable, "Table %s is not an Iceberg table", table);
    SparkTable sparkTable = (SparkTable) table;
    return sparkTable.table();
  }

  /**
   * Converts a PartitionSpec to Spark transforms.
   *
   * @param spec a PartitionSpec
   * @return an array of Transforms
   */
  public static Transform[] toTransforms(PartitionSpec spec) {
    List<Transform> transforms =
        PartitionSpecVisitor.visit(
            spec,
            new PartitionSpecVisitor<Transform>() {
              @Override
              public Transform identity(String sourceName, int sourceId) {
                return Expressions.identity(sourceName);
              }

              @Override
              public Transform bucket(String sourceName, int sourceId, int numBuckets) {
                return Expressions.bucket(numBuckets, sourceName);
              }

              @Override
              public Transform truncate(String sourceName, int sourceId, int width) {
                return Expressions.apply(
                    "truncate", Expressions.column(sourceName), Expressions.literal(width));
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

              @Override
              public Transform alwaysNull(int fieldId, String sourceName, int sourceId) {
                // do nothing for alwaysNull, it doesn't need to be converted to a transform
                return null;
              }

              @Override
              public Transform unknown(
                  int fieldId, String sourceName, int sourceId, String transform) {
                return Expressions.apply(transform, Expressions.column(sourceName));
              }
            });

    return transforms.stream().filter(Objects::nonNull).toArray(Transform[]::new);
  }

  public static Distribution buildRequiredDistribution(org.apache.iceberg.Table table) {
    DistributionMode distributionMode = distributionModeFor(table);
    switch (distributionMode) {
      case NONE:
        return Distributions.unspecified();
      case HASH:
        if (table.spec().isUnpartitioned()) {
          return Distributions.unspecified();
        } else {
          return Distributions.clustered(toTransforms(table.spec()));
        }
      case RANGE:
        if (table.spec().isUnpartitioned() && table.sortOrder().isUnsorted()) {
          return Distributions.unspecified();
        } else {
          org.apache.iceberg.SortOrder requiredSortOrder = SortOrderUtil.buildSortOrder(table);
          return Distributions.ordered(convert(requiredSortOrder));
        }
      default:
        throw new IllegalArgumentException("Unsupported distribution mode: " + distributionMode);
    }
  }

  public static SortOrder[] buildRequiredOrdering(
      Distribution distribution, org.apache.iceberg.Table table) {
    if (distribution instanceof OrderedDistribution) {
      OrderedDistribution orderedDistribution = (OrderedDistribution) distribution;
      return orderedDistribution.ordering();
    } else {
      org.apache.iceberg.SortOrder requiredSortOrder = SortOrderUtil.buildSortOrder(table);
      return convert(requiredSortOrder);
    }
  }

  public static DistributionMode distributionModeFor(org.apache.iceberg.Table table) {
    boolean isSortedTable = !table.sortOrder().isUnsorted();
    String defaultModeName =
        isSortedTable ? WRITE_DISTRIBUTION_MODE_RANGE : WRITE_DISTRIBUTION_MODE_NONE;
    String modeName = table.properties().getOrDefault(WRITE_DISTRIBUTION_MODE, defaultModeName);
    return DistributionMode.fromName(modeName);
  }

  public static SortOrder[] convert(org.apache.iceberg.SortOrder sortOrder) {
    List<OrderField> converted = SortOrderVisitor.visit(sortOrder, new SortOrderToSpark());
    return converted.toArray(new OrderField[0]);
  }

  public static Term toIcebergTerm(Transform transform) {
    Preconditions.checkArgument(
        transform.references().length == 1,
        "Cannot convert transform with more than one column reference: %s",
        transform);
    String colName = DOT.join(transform.references()[0].fieldNames());
    switch (transform.name().toLowerCase(Locale.ROOT)) {
      case "identity":
        return org.apache.iceberg.expressions.Expressions.ref(colName);
      case "bucket":
        return org.apache.iceberg.expressions.Expressions.bucket(colName, findWidth(transform));
      case "years":
        return org.apache.iceberg.expressions.Expressions.year(colName);
      case "months":
        return org.apache.iceberg.expressions.Expressions.month(colName);
      case "date":
      case "days":
        return org.apache.iceberg.expressions.Expressions.day(colName);
      case "date_hour":
      case "hours":
        return org.apache.iceberg.expressions.Expressions.hour(colName);
      case "truncate":
        return org.apache.iceberg.expressions.Expressions.truncate(colName, findWidth(transform));
      default:
        throw new UnsupportedOperationException("Transform is not supported: " + transform);
    }
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
      Preconditions.checkArgument(
          transform.references().length == 1,
          "Cannot convert transform with more than one column reference: %s",
          transform);
      String colName = DOT.join(transform.references()[0].fieldNames());
      switch (transform.name().toLowerCase(Locale.ROOT)) {
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
          Preconditions.checkArgument(
              lit.value() > 0, "Unsupported width for transform: %s", transform.describe());
          return lit.value();

        } else if (((Literal) expr).dataType() instanceof LongType) {
          Literal<Long> lit = (Literal<Long>) expr;
          Preconditions.checkArgument(
              lit.value() > 0 && lit.value() < Integer.MAX_VALUE,
              "Unsupported width for transform: %s",
              transform.describe());
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
    Preconditions.checkArgument(
        fieldNames.length > 0, "Invalid field name: at least one name is required");
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

  public static String describe(org.apache.iceberg.SortOrder order) {
    return Joiner.on(", ").join(SortOrderVisitor.visit(order, DescribeSortOrderVisitor.INSTANCE));
  }

  public static class DescribeSchemaVisitor extends TypeUtil.SchemaVisitor<String> {
    private static final Joiner COMMA = Joiner.on(',');
    private static final DescribeSchemaVisitor INSTANCE = new DescribeSchemaVisitor();

    private DescribeSchemaVisitor() {}

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
      return "list<" + elementResult + ">";
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

  private static class DescribeExpressionVisitor
      extends ExpressionVisitors.ExpressionVisitor<String> {
    private static final DescribeExpressionVisitor INSTANCE = new DescribeExpressionVisitor();

    private DescribeExpressionVisitor() {}

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
        case IS_NAN:
          return "is_nan(" + pred.ref().name() + ")";
        case NOT_NAN:
          return "not_nan(" + pred.ref().name() + ")";
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
        case NOT_STARTS_WITH:
          return pred.ref().name() + " NOT LIKE '" + pred.literal() + "%'";
        case IN:
          return pred.ref().name() + " IN (" + sqlString(pred.literals()) + ")";
        case NOT_IN:
          return pred.ref().name() + " NOT IN (" + sqlString(pred.literals()) + ")";
        default:
          throw new UnsupportedOperationException("Cannot convert predicate to SQL: " + pred);
      }
    }

    private static <T> String sqlString(List<org.apache.iceberg.expressions.Literal<T>> literals) {
      return literals.stream()
          .map(DescribeExpressionVisitor::sqlString)
          .collect(Collectors.joining(", "));
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

  /**
   * Returns a metadata table as a Dataset based on the given Iceberg table.
   *
   * @param spark SparkSession where the Dataset will be created
   * @param table an Iceberg table
   * @param type the type of metadata table
   * @return a Dataset that will read the metadata table
   */
  private static Dataset<Row> loadMetadataTable(
      SparkSession spark, org.apache.iceberg.Table table, MetadataTableType type) {
    Table metadataTable =
        new SparkTable(MetadataTableUtils.createMetadataTableInstance(table, type), false);
    return Dataset.ofRows(
        spark, DataSourceV2Relation.create(metadataTable, Some.empty(), Some.empty()));
  }

  /**
   * Returns an Iceberg Table by its name from a Spark V2 Catalog. If cache is enabled in {@link
   * SparkCatalog}, the {@link TableOperations} of the table may be stale, please refresh the table
   * to get the latest one.
   *
   * @param spark SparkSession used for looking up catalog references and tables
   * @param name The multipart identifier of the Iceberg table
   * @return an Iceberg table
   */
  public static org.apache.iceberg.Table loadIcebergTable(SparkSession spark, String name)
      throws ParseException, NoSuchTableException {
    CatalogAndIdentifier catalogAndIdentifier = catalogAndIdentifier(spark, name);

    TableCatalog catalog = asTableCatalog(catalogAndIdentifier.catalog);
    Table sparkTable = catalog.loadTable(catalogAndIdentifier.identifier);
    return toIcebergTable(sparkTable);
  }

  public static CatalogAndIdentifier catalogAndIdentifier(SparkSession spark, String name)
      throws ParseException {
    return catalogAndIdentifier(
        spark, name, spark.sessionState().catalogManager().currentCatalog());
  }

  public static CatalogAndIdentifier catalogAndIdentifier(
      SparkSession spark, String name, CatalogPlugin defaultCatalog) throws ParseException {
    ParserInterface parser = spark.sessionState().sqlParser();
    Seq<String> multiPartIdentifier = parser.parseMultipartIdentifier(name);
    List<String> javaMultiPartIdentifier = JavaConverters.seqAsJavaList(multiPartIdentifier);
    return catalogAndIdentifier(spark, javaMultiPartIdentifier, defaultCatalog);
  }

  public static CatalogAndIdentifier catalogAndIdentifier(
      String description, SparkSession spark, String name) {
    return catalogAndIdentifier(
        description, spark, name, spark.sessionState().catalogManager().currentCatalog());
  }

  public static CatalogAndIdentifier catalogAndIdentifier(
      String description, SparkSession spark, String name, CatalogPlugin defaultCatalog) {
    try {
      return catalogAndIdentifier(spark, name, defaultCatalog);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Cannot parse " + description + ": " + name, e);
    }
  }

  public static CatalogAndIdentifier catalogAndIdentifier(
      SparkSession spark, List<String> nameParts) {
    return catalogAndIdentifier(
        spark, nameParts, spark.sessionState().catalogManager().currentCatalog());
  }

  /**
   * A modified version of Spark's LookupCatalog.CatalogAndIdentifier.unapply Attempts to find the
   * catalog and identifier a multipart identifier represents
   *
   * @param spark Spark session to use for resolution
   * @param nameParts Multipart identifier representing a table
   * @param defaultCatalog Catalog to use if none is specified
   * @return The CatalogPlugin and Identifier for the table
   */
  public static CatalogAndIdentifier catalogAndIdentifier(
      SparkSession spark, List<String> nameParts, CatalogPlugin defaultCatalog) {
    CatalogManager catalogManager = spark.sessionState().catalogManager();

    String[] currentNamespace;
    if (defaultCatalog.equals(catalogManager.currentCatalog())) {
      currentNamespace = catalogManager.currentNamespace();
    } else {
      currentNamespace = defaultCatalog.defaultNamespace();
    }

    Pair<CatalogPlugin, Identifier> catalogIdentifier =
        SparkUtil.catalogAndIdentifier(
            nameParts,
            catalogName -> {
              try {
                return catalogManager.catalog(catalogName);
              } catch (Exception e) {
                return null;
              }
            },
            Identifier::of,
            defaultCatalog,
            currentNamespace);
    return new CatalogAndIdentifier(catalogIdentifier);
  }

  private static TableCatalog asTableCatalog(CatalogPlugin catalog) {
    if (catalog instanceof TableCatalog) {
      return (TableCatalog) catalog;
    }

    throw new IllegalArgumentException(
        String.format(
            "Cannot use catalog %s(%s): not a TableCatalog",
            catalog.name(), catalog.getClass().getName()));
  }

  /** This mimics a class inside of Spark which is private inside of LookupCatalog. */
  public static class CatalogAndIdentifier {
    private final CatalogPlugin catalog;
    private final Identifier identifier;

    public CatalogAndIdentifier(CatalogPlugin catalog, Identifier identifier) {
      this.catalog = catalog;
      this.identifier = identifier;
    }

    public CatalogAndIdentifier(Pair<CatalogPlugin, Identifier> identifier) {
      this.catalog = identifier.first();
      this.identifier = identifier.second();
    }

    public CatalogPlugin catalog() {
      return catalog;
    }

    public Identifier identifier() {
      return identifier;
    }
  }

  public static TableIdentifier identifierToTableIdentifier(Identifier identifier) {
    return TableIdentifier.of(Namespace.of(identifier.namespace()), identifier.name());
  }

  /**
   * Use Spark to list all partitions in the table.
   *
   * @param spark a Spark session
   * @param rootPath a table identifier
   * @param format format of the file
   * @return all table's partitions
   */
  public static List<SparkPartition> getPartitions(
      SparkSession spark, Path rootPath, String format) {
    FileStatusCache fileStatusCache = FileStatusCache.getOrCreate(spark);
    Map<String, String> emptyMap = Collections.emptyMap();

    InMemoryFileIndex fileIndex =
        new InMemoryFileIndex(
            spark,
            JavaConverters.collectionAsScalaIterableConverter(ImmutableList.of(rootPath))
                .asScala()
                .toSeq(),
            JavaConverters.mapAsScalaMapConverter(emptyMap).asScala().toMap(Predef.conforms()),
            Option.empty(),
            fileStatusCache,
            Option.empty(),
            Option.empty());

    org.apache.spark.sql.execution.datasources.PartitionSpec spec = fileIndex.partitionSpec();
    StructType schema = spec.partitionColumns();

    return JavaConverters.seqAsJavaListConverter(spec.partitions()).asJava().stream()
        .map(
            partition -> {
              Map<String, String> values = Maps.newHashMap();
              JavaConverters.asJavaIterableConverter(schema)
                  .asJava()
                  .forEach(
                      field -> {
                        int fieldIndex = schema.fieldIndex(field.name());
                        Object catalystValue = partition.values().get(fieldIndex, field.dataType());
                        Object value =
                            CatalystTypeConverters.convertToScala(catalystValue, field.dataType());
                        values.put(field.name(), String.valueOf(value));
                      });
              return new SparkPartition(values, partition.path().toString(), format);
            })
        .collect(Collectors.toList());
  }

  public static org.apache.spark.sql.catalyst.TableIdentifier toV1TableIdentifier(
      Identifier identifier) {
    String[] namespace = identifier.namespace();

    Preconditions.checkArgument(
        namespace.length <= 1,
        "Cannot convert %s to a Spark v1 identifier, namespace contains more than 1 part",
        identifier);

    String table = identifier.name();
    Option<String> database = namespace.length == 1 ? Option.apply(namespace[0]) : Option.empty();
    return org.apache.spark.sql.catalyst.TableIdentifier.apply(table, database);
  }

  private static class DescribeSortOrderVisitor implements SortOrderVisitor<String> {
    private static final DescribeSortOrderVisitor INSTANCE = new DescribeSortOrderVisitor();

    private DescribeSortOrderVisitor() {}

    @Override
    public String field(
        String sourceName,
        int sourceId,
        org.apache.iceberg.SortDirection direction,
        NullOrder nullOrder) {
      return String.format("%s %s %s", sourceName, direction, nullOrder);
    }

    @Override
    public String bucket(
        String sourceName,
        int sourceId,
        int numBuckets,
        org.apache.iceberg.SortDirection direction,
        NullOrder nullOrder) {
      return String.format("bucket(%s, %s) %s %s", numBuckets, sourceName, direction, nullOrder);
    }

    @Override
    public String truncate(
        String sourceName,
        int sourceId,
        int width,
        org.apache.iceberg.SortDirection direction,
        NullOrder nullOrder) {
      return String.format("truncate(%s, %s) %s %s", sourceName, width, direction, nullOrder);
    }

    @Override
    public String year(
        String sourceName,
        int sourceId,
        org.apache.iceberg.SortDirection direction,
        NullOrder nullOrder) {
      return String.format("years(%s) %s %s", sourceName, direction, nullOrder);
    }

    @Override
    public String month(
        String sourceName,
        int sourceId,
        org.apache.iceberg.SortDirection direction,
        NullOrder nullOrder) {
      return String.format("months(%s) %s %s", sourceName, direction, nullOrder);
    }

    @Override
    public String day(
        String sourceName,
        int sourceId,
        org.apache.iceberg.SortDirection direction,
        NullOrder nullOrder) {
      return String.format("days(%s) %s %s", sourceName, direction, nullOrder);
    }

    @Override
    public String hour(
        String sourceName,
        int sourceId,
        org.apache.iceberg.SortDirection direction,
        NullOrder nullOrder) {
      return String.format("hours(%s) %s %s", sourceName, direction, nullOrder);
    }

    @Override
    public String unknown(
        String sourceName,
        int sourceId,
        String transform,
        org.apache.iceberg.SortDirection direction,
        NullOrder nullOrder) {
      return String.format("%s(%s) %s %s", transform, sourceName, direction, nullOrder);
    }
  }
}
