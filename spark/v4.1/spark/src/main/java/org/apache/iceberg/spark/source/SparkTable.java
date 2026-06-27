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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.CommitMetadata;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkV2Filters;
import org.apache.iceberg.spark.TimeTravel;
import org.apache.iceberg.spark.TimeTravel.AsOfTimestamp;
import org.apache.iceberg.spark.TimeTravel.AsOfVersion;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.SupportsAtomicPartitionManagement;
import org.apache.spark.sql.connector.catalog.SupportsDeleteV2;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.constraints.Constraint;
import org.apache.spark.sql.connector.catalog.constraints.Constraint.ValidationStatus;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main Spark table implementation that supports reads, writes, and row-level operations.
 *
 * <p>Note the table state (e.g. schema, snapshot) is pinned upon loading and must not change.
 */
public class SparkTable extends BaseSparkTable
    implements SupportsRead,
        SupportsWrite,
        SupportsDeleteV2,
        SupportsRowLevelOperations,
        SupportsAtomicPartitionManagement {

  private static final Logger LOG = LoggerFactory.getLogger(SparkTable.class);

  private static final Set<TableCapability> BASE_CAPABILITIES =
      ImmutableSet.of(
          TableCapability.BATCH_READ,
          TableCapability.BATCH_WRITE,
          TableCapability.MICRO_BATCH_READ,
          TableCapability.STREAMING_WRITE,
          TableCapability.OVERWRITE_BY_FILTER,
          TableCapability.OVERWRITE_DYNAMIC);

  private final Schema schema; // effective schema (not necessarily current table schema)
  private final Snapshot snapshot; // always set unless table is empty
  private final String branch; // set if table is loaded for specific branch
  private final TimeTravel timeTravel; // set if table is loaded for time travel
  private final Set<TableCapability> capabilities;

  public SparkTable(Table table) {
    this(table, null /* main branch */);
  }

  private SparkTable(Table table, String branch) {
    this(
        table,
        table.schema(),
        determineLatestSnapshot(table, branch),
        branch,
        null /* no time travel */);
  }

  private SparkTable(Table table, long snapshotId, TimeTravel timeTravel) {
    this(
        table,
        SnapshotUtil.schemaFor(table, snapshotId),
        table.snapshot(snapshotId),
        null /* main branch */,
        timeTravel);
  }

  private SparkTable(
      Table table, Schema schema, Snapshot snapshot, String branch, TimeTravel timeTravel) {
    super(table, schema);
    this.schema = schema;
    this.snapshot = snapshot;
    this.branch = branch;
    this.timeTravel = timeTravel;
    this.capabilities = computeCapabilities(table);
  }

  public SparkTable copyWithBranch(String newBranch) {
    return new SparkTable(table(), newBranch);
  }

  public Long snapshotId() {
    return snapshot != null ? snapshot.snapshotId() : null;
  }

  public String branch() {
    return branch;
  }

  @Override
  public String id() {
    return Spark3Util.baseTableUUID(table());
  }

  @Override
  public String version() {
    return String.format("branch_%s_snapshot_%s", branch, snapshotId());
  }

  @Override
  public Set<TableCapability> capabilities() {
    return capabilities;
  }

  @Override
  public Constraint[] constraints() {
    List<Constraint> constraints = Lists.newArrayList();

    SparkReadConf readConf = new SparkReadConf(spark(), table());
    Set<String> identifierFieldNames = schema.identifierFieldNames();

    if (readConf.identifierFieldsRely() && !identifierFieldNames.isEmpty()) {
      constraints.add(
          Constraint.primaryKey("iceberg_pk", Spark3Util.toNamedReferences(identifierFieldNames))
              .enforced(false)
              .validationStatus(ValidationStatus.UNVALIDATED)
              .rely(true)
              .build());
    }

    return constraints.toArray(new Constraint[0]);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new SparkScanBuilder(spark(), table(), schema, snapshot, branch, timeTravel, options);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    Preconditions.checkArgument(timeTravel == null, "Cannot write to table with time travel");
    return new SparkWriteBuilder(spark(), table(), branch, info);
  }

  @Override
  public RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info) {
    Preconditions.checkArgument(timeTravel == null, "Cannot modify table with time travel");
    return new SparkRowLevelOperationBuilder(spark(), table(), snapshot, branch, info);
  }

  @Override
  public boolean canDeleteWhere(Predicate[] predicates) {
    Preconditions.checkArgument(timeTravel == null, "Cannot delete from table with time travel");

    Expression deleteExpr = Expressions.alwaysTrue();

    for (Predicate predicate : predicates) {
      Expression expr = SparkV2Filters.convert(predicate);
      if (expr != null) {
        deleteExpr = Expressions.and(deleteExpr, expr);
      } else {
        return false;
      }
    }

    String scanBranch =
        SparkTableUtil.determineReadBranch(
            spark(), table(), branch, CaseInsensitiveStringMap.empty());
    return canDeleteUsingMetadata(deleteExpr, scanBranch);
  }

  // a metadata delete is possible iff matching files can be deleted entirely
  private boolean canDeleteUsingMetadata(Expression deleteExpr, String scanBranch) {
    boolean caseSensitive = SparkUtil.caseSensitive(spark());

    if (ExpressionUtil.selectsPartitions(deleteExpr, table(), caseSensitive)) {
      return true;
    }

    TableScan scan =
        table()
            .newScan()
            .filter(deleteExpr)
            .caseSensitive(caseSensitive)
            .includeColumnStats()
            .ignoreResiduals();

    if (scanBranch != null) {
      scan = scan.useRef(scanBranch);
    } else if (snapshot != null) {
      scan = scan.useSnapshot(snapshot.snapshotId());
    }

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Map<Integer, Evaluator> evaluators = Maps.newHashMap();
      StrictMetricsEvaluator metricsEvaluator = new StrictMetricsEvaluator(schema, deleteExpr);
      return Iterables.all(
          tasks,
          task -> {
            DataFile file = task.file();
            PartitionSpec spec = task.spec();
            Evaluator evaluator =
                evaluators.computeIfAbsent(
                    spec.specId(),
                    specId ->
                        new Evaluator(
                            spec.partitionType(), Projections.strict(spec).project(deleteExpr)));
            return evaluator.eval(file.partition()) || metricsEvaluator.eval(file);
          });

    } catch (IOException ioe) {
      LOG.warn("Failed to close task iterable", ioe);
      return false;
    }
  }

  @Override
  public void deleteWhere(Predicate[] predicates) {
    Expression deleteExpr = SparkV2Filters.convert(predicates);

    if (deleteExpr == Expressions.alwaysFalse()) {
      LOG.info("Skipping the delete operation as the condition is always false");
      return;
    }

    DeleteFiles deleteFiles =
        table()
            .newDelete()
            .set("spark.app.id", spark().sparkContext().applicationId())
            .deleteFromRowFilter(deleteExpr);

    String writeBranch =
        SparkTableUtil.determineWriteBranch(
            spark(), table(), branch, CaseInsensitiveStringMap.empty());

    if (writeBranch != null) {
      deleteFiles.toBranch(writeBranch);
    }

    if (!CommitMetadata.commitProperties().isEmpty()) {
      CommitMetadata.commitProperties().forEach(deleteFiles::set);
    }

    deleteFiles.commit();
  }

  // Iceberg has no per-partition metastore state, partitions are derived from data file metadata
  // in manifests. Only DROP PARTITION is supported here: partitionSchema exposes the partition
  // fields from the current PartitionSpec (e.g. ts_day, id_bucket) and DROP PARTITION maps to a
  // snapshot-level delete with a transform-aware row filter, so the user always names a whole
  // physical partition.

  @Override
  public StructType partitionSchema() {
    List<StructField> fields = Lists.newArrayList();
    for (PartitionField field : table().spec().fields()) {
      if (field.transform().equals(Transforms.alwaysNull())) {
        continue;
      }
      Types.NestedField sourceField = table().schema().findField(field.sourceId());
      DataType sparkType;
      if (isTimeTransform(field.transform())) {
        // Time transforms (year/month/day/hour) are surfaced as their human-readable string
        // forms (e.g. "2024", "2024-02", "2024-02-15", "2024-02-15-11") so users can write a
        // partition value that mirrors the on-disk partition path.
        sparkType = DataTypes.StringType;
      } else {
        Type resultType = field.transform().getResultType(sourceField.type());
        sparkType = SparkSchemaUtil.convert(resultType);
      }
      fields.add(
          new StructField(field.name(), sparkType, sourceField.isOptional(), Metadata.empty()));
    }
    return new StructType(fields.toArray(new StructField[0]));
  }

  private static boolean isTimeTransform(Transform<?, ?> transform) {
    String name = transform.toString();
    return "year".equals(name) || "month".equals(name) || "day".equals(name) || "hour".equals(name);
  }

  @Override
  public boolean partitionExists(InternalRow ident) {
    return partitionHasFiles(partitionRowFilter(ident));
  }

  @Override
  public boolean dropPartition(InternalRow ident) {
    Expression filter = partitionRowFilter(ident);
    if (!partitionHasFiles(filter)) {
      return false;
    }
    table().newDelete().deleteFromRowFilter(filter).commit();
    return true;
  }

  @Override
  public boolean dropPartitions(InternalRow[] idents) {
    if (idents.length == 0) {
      return false;
    }
    Expression filter = Expressions.alwaysFalse();
    for (InternalRow ident : idents) {
      filter = Expressions.or(filter, partitionRowFilter(ident));
    }
    if (!partitionHasFiles(filter)) {
      return false;
    }
    table().newDelete().deleteFromRowFilter(filter).commit();
    return true;
  }

  @Override
  public void createPartition(InternalRow ident, Map<String, String> properties) {
    throw new UnsupportedOperationException(
        "Iceberg does not support creating partitions explicitly; they are derived from data files");
  }

  @Override
  public void createPartitions(InternalRow[] idents, Map<String, String>[] properties) {
    throw new UnsupportedOperationException(
        "Iceberg does not support creating partitions explicitly; they are derived from data files");
  }

  @Override
  public void replacePartitionMetadata(InternalRow ident, Map<String, String> properties) {
    throw new UnsupportedOperationException("Iceberg does not support per-partition metadata");
  }

  @Override
  public Map<String, String> loadPartitionMetadata(InternalRow ident) {
    throw new UnsupportedOperationException("Iceberg does not support per-partition metadata");
  }

  @Override
  public InternalRow[] listPartitionIdentifiers(String[] names, InternalRow ident) {
    throw new UnsupportedOperationException(
        "Listing partition identifiers is not implemented for Iceberg tables");
  }

  private Expression partitionRowFilter(InternalRow ident) {
    StructType partitionSchema = partitionSchema();
    Preconditions.checkArgument(
        ident.numFields() == partitionSchema.length(),
        "Partition row width %s does not match partition schema width %s",
        ident.numFields(),
        partitionSchema.length());

    Map<String, PartitionField> partFieldsByName = Maps.newLinkedHashMap();
    for (PartitionField field : table().spec().fields()) {
      if (!field.transform().equals(Transforms.alwaysNull())) {
        partFieldsByName.put(field.name(), field);
      }
    }

    Expression filter = Expressions.alwaysTrue();
    StructField[] fields = partitionSchema.fields();
    for (int i = 0; i < fields.length; i++) {
      StructField field = fields[i];
      PartitionField partField = partFieldsByName.get(field.name());
      String sourceName = table().schema().findColumnName(partField.sourceId());
      Object value = ident.isNullAt(i) ? null : ident.get(i, field.dataType());
      Object javaValue = catalystToJava(value);
      filter = Expressions.and(filter, partitionFieldPredicate(partField, sourceName, javaValue));
    }
    return filter;
  }

  private static Expression partitionFieldPredicate(
      PartitionField partField, String sourceName, Object value) {
    Transform<?, ?> transform = partField.transform();
    String transformName = transform.toString();
    if ("identity".equals(transformName)) {
      return (value == null)
          ? Expressions.isNull(sourceName)
          : Expressions.equal(sourceName, value);
    }
    UnboundTerm<Object> term = constructTerm(sourceName, transformName);
    Object termValue = (value == null) ? null : parseHumanDateIfNeeded(value, transformName);

    return (termValue == null) ? Expressions.isNull(term) : Expressions.equal(term, termValue);
  }

  private static Object parseHumanDateIfNeeded(@Nonnull Object value, String transformName) {
    Object termValue = value;
    if ("year".equals(transformName)) {
      termValue = Transforms.parseHumanYear(value.toString());
    } else if ("month".equals(transformName)) {
      termValue = Transforms.parseHumanMonth(value.toString());
    } else if ("day".equals(transformName)) {
      termValue = Transforms.parseHumanDay(value.toString());
    } else if ("hour".equals(transformName)) {
      termValue = Transforms.parseHumanHour(value.toString());
    }

    return termValue;
  }

  private static UnboundTerm<Object> constructTerm(String sourceName, String transformName) {
    UnboundTerm<Object> term;
    if ("year".equals(transformName)) {
      term = Expressions.year(sourceName);
    } else if ("month".equals(transformName)) {
      term = Expressions.month(sourceName);
    } else if ("day".equals(transformName)) {
      term = Expressions.day(sourceName);
    } else if ("hour".equals(transformName)) {
      term = Expressions.hour(sourceName);
    } else if (transformName.startsWith("bucket[") && transformName.endsWith("]")) {
      int numBuckets = parseTransformArg(transformName, "bucket[");
      term = Expressions.bucket(sourceName, numBuckets);
    } else if (transformName.startsWith("truncate[") && transformName.endsWith("]")) {
      int width = parseTransformArg(transformName, "truncate[");
      term = Expressions.truncate(sourceName, width);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported partition transform for DROP PARTITION: " + transformName);
    }

    return term;
  }

  private static int parseTransformArg(String transformName, String prefix) {
    return Integer.parseInt(transformName.substring(prefix.length(), transformName.length() - 1));
  }

  private static Object catalystToJava(Object value) {
    if (value instanceof UTF8String) {
      return value.toString();
    } else if (value instanceof Decimal) {
      return ((Decimal) value).toJavaBigDecimal();
    }
    return value;
  }

  private boolean partitionHasFiles(Expression filter) {
    try (CloseableIterable<FileScanTask> tasks =
        table().newScan().filter(filter).ignoreResiduals().planFiles()) {
      return tasks.iterator().hasNext();
    } catch (IOException e) {
      LOG.warn("Failed to close file scan iterable while checking partition existence", e);
      return true;
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    SparkTable that = (SparkTable) other;
    return table().name().equals(that.table().name())
        && Objects.equals(table().uuid(), that.table().uuid())
        && schema.schemaId() == that.schema.schemaId()
        && Objects.equals(snapshotId(), that.snapshotId())
        && Objects.equals(branch, that.branch)
        && Objects.equals(timeTravel, that.timeTravel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(), table().uuid(), schema.schemaId(), snapshotId(), branch, timeTravel);
  }

  public static SparkTable create(Table table, String branch) {
    ValidationException.check(
        branch == null || SnapshotRef.MAIN_BRANCH.equals(branch) || table.snapshot(branch) != null,
        "Cannot use branch (does not exist): %s",
        branch);
    return new SparkTable(table, branch);
  }

  public static SparkTable create(Table table, TimeTravel timeTravel) {
    if (timeTravel == null) {
      return new SparkTable(table);
    } else if (timeTravel instanceof AsOfVersion asOfVersion) {
      return createWithVersion(table, asOfVersion);
    } else if (timeTravel instanceof AsOfTimestamp asOfTimestamp) {
      return createWithTimestamp(table, asOfTimestamp);
    } else {
      throw new IllegalArgumentException("Unknown time travel: " + timeTravel);
    }
  }

  private static SparkTable createWithVersion(Table table, AsOfVersion timeTravel) {
    if (timeTravel.isSnapshotId()) {
      return new SparkTable(table, Long.parseLong(timeTravel.version()), timeTravel);
    } else {
      SnapshotRef ref = table.refs().get(timeTravel.version());
      Preconditions.checkArgument(
          ref != null,
          "Cannot find matching snapshot ID or reference name for version %s",
          timeTravel.version());
      if (ref.isBranch()) {
        return new SparkTable(table, timeTravel.version());
      } else {
        return new SparkTable(table, ref.snapshotId(), timeTravel);
      }
    }
  }

  // Iceberg uses milliseconds for snapshot timestamps
  private static SparkTable createWithTimestamp(Table table, AsOfTimestamp timeTravel) {
    long timestampMillis = timeTravel.timestampMillis();
    long snapshotId = SnapshotUtil.snapshotIdAsOfTime(table, timestampMillis);
    return new SparkTable(table, snapshotId, timeTravel);
  }

  private static Set<TableCapability> computeCapabilities(Table table) {
    ImmutableSet.Builder<TableCapability> tableCapabilities = ImmutableSet.builder();
    tableCapabilities.addAll(BASE_CAPABILITIES);

    if (autoSchemaEvolution(table)) {
      tableCapabilities.add(TableCapability.AUTOMATIC_SCHEMA_EVOLUTION);
    }

    if (acceptAnySchema(table)) {
      tableCapabilities.add(TableCapability.ACCEPT_ANY_SCHEMA);
    }

    return tableCapabilities.build();
  }

  private static boolean acceptAnySchema(Table table) {
    return PropertyUtil.propertyAsBoolean(
        table.properties(),
        TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA,
        TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA_DEFAULT);
  }

  private static boolean autoSchemaEvolution(Table table) {
    return PropertyUtil.propertyAsBoolean(
        table.properties(),
        TableProperties.SPARK_WRITE_AUTO_SCHEMA_EVOLUTION,
        TableProperties.SPARK_WRITE_AUTO_SCHEMA_EVOLUTION_DEFAULT);
  }

  // returns latest snapshot for branch or current snapshot if branch is yet to be created
  private static Snapshot determineLatestSnapshot(Table table, String branch) {
    if (branch != null && table.refs().containsKey(branch)) {
      return SnapshotUtil.latestSnapshot(table, branch);
    } else {
      return table.currentSnapshot();
    }
  }
}
