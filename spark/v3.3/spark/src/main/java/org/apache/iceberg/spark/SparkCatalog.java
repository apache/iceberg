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

import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.GC_ENABLED_DEFAULT;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.source.SparkChangelogTable;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.spark.source.StagedSparkTable;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.TableChange.ColumnChange;
import org.apache.spark.sql.connector.catalog.TableChange.RemoveProperty;
import org.apache.spark.sql.connector.catalog.TableChange.SetProperty;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A Spark TableCatalog implementation that wraps an Iceberg {@link Catalog}.
 *
 * <p>This supports the following catalog configuration options:
 *
 * <ul>
 *   <li><code>type</code> - catalog type, "hive" or "hadoop". To specify a non-hive or hadoop
 *       catalog, use the <code>catalog-impl</code> option.
 *   <li><code>uri</code> - the Hive Metastore URI (Hive catalog only)
 *   <li><code>warehouse</code> - the warehouse path (Hadoop catalog only)
 *   <li><code>catalog-impl</code> - a custom {@link Catalog} implementation to use
 *   <li><code>default-namespace</code> - a namespace to use as the default
 *   <li><code>cache-enabled</code> - whether to enable catalog cache
 *   <li><code>cache.expiration-interval-ms</code> - interval in millis before expiring tables from
 *       catalog cache. Refer to {@link CatalogProperties#CACHE_EXPIRATION_INTERVAL_MS} for further
 *       details and significant values.
 * </ul>
 *
 * <p>
 */
public class SparkCatalog extends BaseCatalog {
  private static final Set<String> DEFAULT_NS_KEYS = ImmutableSet.of(TableCatalog.PROP_OWNER);
  private static final Splitter COMMA = Splitter.on(",");
  private static final Pattern AT_TIMESTAMP = Pattern.compile("at_timestamp_(\\d+)");
  private static final Pattern SNAPSHOT_ID = Pattern.compile("snapshot_id_(\\d+)");

  private String catalogName = null;
  private Catalog icebergCatalog = null;
  private boolean cacheEnabled = CatalogProperties.CACHE_ENABLED_DEFAULT;
  private SupportsNamespaces asNamespaceCatalog = null;
  private String[] defaultNamespace = null;
  private HadoopTables tables;
  private boolean useTimestampsWithoutZone;

  /**
   * Build an Iceberg {@link Catalog} to be used by this Spark catalog adapter.
   *
   * @param name Spark's catalog name
   * @param options Spark's catalog options
   * @return an Iceberg catalog
   */
  protected Catalog buildIcebergCatalog(String name, CaseInsensitiveStringMap options) {
    Configuration conf = SparkUtil.hadoopConfCatalogOverrides(SparkSession.active(), name);
    Map<String, String> optionsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    optionsMap.putAll(options.asCaseSensitiveMap());
    optionsMap.put(CatalogProperties.APP_ID, SparkSession.active().sparkContext().applicationId());
    optionsMap.put(CatalogProperties.USER, SparkSession.active().sparkContext().sparkUser());
    return CatalogUtil.buildIcebergCatalog(name, optionsMap, conf);
  }

  /**
   * Build an Iceberg {@link TableIdentifier} for the given Spark identifier.
   *
   * @param identifier Spark's identifier
   * @return an Iceberg identifier
   */
  protected TableIdentifier buildIdentifier(Identifier identifier) {
    return Spark3Util.identifierToTableIdentifier(identifier);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    try {
      return load(ident);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    Table table = loadTable(ident);

    if (table instanceof SparkTable) {
      SparkTable sparkTable = (SparkTable) table;

      Preconditions.checkArgument(
          sparkTable.snapshotId() == null,
          "Cannot do time-travel based on both table identifier and AS OF");

      return sparkTable.copyWithSnapshotId(Long.parseLong(version));

    } else if (table instanceof SparkChangelogTable) {
      throw new UnsupportedOperationException("AS OF is not supported for changelogs");

    } else {
      throw new IllegalArgumentException("Unknown Spark table type: " + table.getClass().getName());
    }
  }

  @Override
  public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
    Table table = loadTable(ident);

    if (table instanceof SparkTable) {
      SparkTable sparkTable = (SparkTable) table;

      Preconditions.checkArgument(
          sparkTable.snapshotId() == null,
          "Cannot do time-travel based on both table identifier and AS OF");

      // convert the timestamp to milliseconds as Spark passes microseconds
      // but Iceberg uses milliseconds for snapshot timestamps
      long timestampMillis = TimeUnit.MICROSECONDS.toMillis(timestamp);
      long snapshotId = SnapshotUtil.snapshotIdAsOfTime(sparkTable.table(), timestampMillis);
      return sparkTable.copyWithSnapshotId(snapshotId);

    } else if (table instanceof SparkChangelogTable) {
      throw new UnsupportedOperationException("AS OF is not supported for changelogs");

    } else {
      throw new IllegalArgumentException("Unknown Spark table type: " + table.getClass().getName());
    }
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException {
    Schema icebergSchema = SparkSchemaUtil.convert(schema, useTimestampsWithoutZone);
    try {
      Catalog.TableBuilder builder = newBuilder(ident, icebergSchema);
      org.apache.iceberg.Table icebergTable =
          builder
              .withPartitionSpec(Spark3Util.toPartitionSpec(icebergSchema, transforms))
              .withLocation(properties.get("location"))
              .withProperties(Spark3Util.rebuildCreateProperties(properties))
              .create();
      return new SparkTable(icebergTable, !cacheEnabled);
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  @Override
  public StagedTable stageCreate(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException {
    Schema icebergSchema = SparkSchemaUtil.convert(schema, useTimestampsWithoutZone);
    try {
      Catalog.TableBuilder builder = newBuilder(ident, icebergSchema);
      Transaction transaction =
          builder
              .withPartitionSpec(Spark3Util.toPartitionSpec(icebergSchema, transforms))
              .withLocation(properties.get("location"))
              .withProperties(Spark3Util.rebuildCreateProperties(properties))
              .createTransaction();
      return new StagedSparkTable(transaction);
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  @Override
  public StagedTable stageReplace(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws NoSuchTableException {
    Schema icebergSchema = SparkSchemaUtil.convert(schema, useTimestampsWithoutZone);
    try {
      Catalog.TableBuilder builder = newBuilder(ident, icebergSchema);
      Transaction transaction =
          builder
              .withPartitionSpec(Spark3Util.toPartitionSpec(icebergSchema, transforms))
              .withLocation(properties.get("location"))
              .withProperties(Spark3Util.rebuildCreateProperties(properties))
              .replaceTransaction();
      return new StagedSparkTable(transaction);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public StagedTable stageCreateOrReplace(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties) {
    Schema icebergSchema = SparkSchemaUtil.convert(schema, useTimestampsWithoutZone);
    Catalog.TableBuilder builder = newBuilder(ident, icebergSchema);
    Transaction transaction =
        builder
            .withPartitionSpec(Spark3Util.toPartitionSpec(icebergSchema, transforms))
            .withLocation(properties.get("location"))
            .withProperties(Spark3Util.rebuildCreateProperties(properties))
            .createOrReplaceTransaction();
    return new StagedSparkTable(transaction);
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    SetProperty setLocation = null;
    SetProperty setSnapshotId = null;
    SetProperty pickSnapshotId = null;
    List<TableChange> propertyChanges = Lists.newArrayList();
    List<TableChange> schemaChanges = Lists.newArrayList();

    for (TableChange change : changes) {
      if (change instanceof SetProperty) {
        SetProperty set = (SetProperty) change;
        if (TableCatalog.PROP_LOCATION.equalsIgnoreCase(set.property())) {
          setLocation = set;
        } else if ("current-snapshot-id".equalsIgnoreCase(set.property())) {
          setSnapshotId = set;
        } else if ("cherry-pick-snapshot-id".equalsIgnoreCase(set.property())) {
          pickSnapshotId = set;
        } else if ("sort-order".equalsIgnoreCase(set.property())) {
          throw new UnsupportedOperationException(
              "Cannot specify the 'sort-order' because it's a reserved table "
                  + "property. Please use the command 'ALTER TABLE ... WRITE ORDERED BY' to specify write sort-orders.");
        } else {
          propertyChanges.add(set);
        }
      } else if (change instanceof RemoveProperty) {
        propertyChanges.add(change);
      } else if (change instanceof ColumnChange) {
        schemaChanges.add(change);
      } else {
        throw new UnsupportedOperationException("Cannot apply unknown table change: " + change);
      }
    }

    try {
      org.apache.iceberg.Table table = icebergCatalog.loadTable(buildIdentifier(ident));
      commitChanges(
          table, setLocation, setSnapshotId, pickSnapshotId, propertyChanges, schemaChanges);
      return new SparkTable(table, true /* refreshEagerly */);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return dropTableWithoutPurging(ident);
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    try {
      org.apache.iceberg.Table table = icebergCatalog.loadTable(buildIdentifier(ident));
      ValidationException.check(
          PropertyUtil.propertyAsBoolean(table.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
          "Cannot purge table: GC is disabled (deleting files may corrupt other tables)");
      String metadataFileLocation =
          ((HasTableOperations) table).operations().current().metadataFileLocation();

      boolean dropped = dropTableWithoutPurging(ident);

      if (dropped) {
        // We should check whether the metadata file exists. Because the HadoopCatalog/HadoopTables
        // will drop the
        // warehouse directly and ignore the `purge` argument.
        boolean metadataFileExists = table.io().newInputFile(metadataFileLocation).exists();

        if (metadataFileExists) {
          SparkActions.get().deleteReachableFiles(metadataFileLocation).io(table.io()).execute();
        }
      }

      return dropped;
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      return false;
    }
  }

  private boolean dropTableWithoutPurging(Identifier ident) {
    if (isPathIdentifier(ident)) {
      return tables.dropTable(((PathIdentifier) ident).location(), false /* don't purge data */);
    } else {
      return icebergCatalog.dropTable(buildIdentifier(ident), false /* don't purge data */);
    }
  }

  @Override
  public void renameTable(Identifier from, Identifier to)
      throws NoSuchTableException, TableAlreadyExistsException {
    try {
      checkNotPathIdentifier(from, "renameTable");
      checkNotPathIdentifier(to, "renameTable");
      icebergCatalog.renameTable(buildIdentifier(from), buildIdentifier(to));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(from);
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(to);
    }
  }

  @Override
  public void invalidateTable(Identifier ident) {
    if (!isPathIdentifier(ident)) {
      icebergCatalog.invalidateTable(buildIdentifier(ident));
    }
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    return icebergCatalog.listTables(Namespace.of(namespace)).stream()
        .map(ident -> Identifier.of(ident.namespace().levels(), ident.name()))
        .toArray(Identifier[]::new);
  }

  @Override
  public String[] defaultNamespace() {
    if (defaultNamespace != null) {
      return defaultNamespace;
    }

    return new String[0];
  }

  @Override
  public String[][] listNamespaces() {
    if (asNamespaceCatalog != null) {
      return asNamespaceCatalog.listNamespaces().stream()
          .map(Namespace::levels)
          .toArray(String[][]::new);
    }

    return new String[0][];
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      try {
        return asNamespaceCatalog.listNamespaces(Namespace.of(namespace)).stream()
            .map(Namespace::levels)
            .toArray(String[][]::new);
      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    }

    throw new NoSuchNamespaceException(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      try {
        return asNamespaceCatalog.loadNamespaceMetadata(Namespace.of(namespace));
      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    }

    throw new NoSuchNamespaceException(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    if (asNamespaceCatalog != null) {
      try {
        if (asNamespaceCatalog instanceof HadoopCatalog
            && DEFAULT_NS_KEYS.equals(metadata.keySet())) {
          // Hadoop catalog will reject metadata properties, but Spark automatically adds "owner".
          // If only the automatic properties are present, replace metadata with an empty map.
          asNamespaceCatalog.createNamespace(Namespace.of(namespace), ImmutableMap.of());
        } else {
          asNamespaceCatalog.createNamespace(Namespace.of(namespace), metadata);
        }
      } catch (AlreadyExistsException e) {
        throw new NamespaceAlreadyExistsException(namespace);
      }
    } else {
      throw new UnsupportedOperationException(
          "Namespaces are not supported by catalog: " + catalogName);
    }
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      Map<String, String> updates = Maps.newHashMap();
      Set<String> removals = Sets.newHashSet();
      for (NamespaceChange change : changes) {
        if (change instanceof NamespaceChange.SetProperty) {
          NamespaceChange.SetProperty set = (NamespaceChange.SetProperty) change;
          updates.put(set.property(), set.value());
        } else if (change instanceof NamespaceChange.RemoveProperty) {
          removals.add(((NamespaceChange.RemoveProperty) change).property());
        } else {
          throw new UnsupportedOperationException(
              "Cannot apply unknown namespace change: " + change);
        }
      }

      try {
        if (!updates.isEmpty()) {
          asNamespaceCatalog.setProperties(Namespace.of(namespace), updates);
        }

        if (!removals.isEmpty()) {
          asNamespaceCatalog.removeProperties(Namespace.of(namespace), removals);
        }

      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    } else {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      try {
        return asNamespaceCatalog.dropNamespace(Namespace.of(namespace));
      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    }

    return false;
  }

  @Override
  public final void initialize(String name, CaseInsensitiveStringMap options) {
    this.cacheEnabled =
        PropertyUtil.propertyAsBoolean(
            options, CatalogProperties.CACHE_ENABLED, CatalogProperties.CACHE_ENABLED_DEFAULT);

    long cacheExpirationIntervalMs =
        PropertyUtil.propertyAsLong(
            options,
            CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS,
            CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS_DEFAULT);

    // An expiration interval of 0ms effectively disables caching.
    // Do not wrap with CachingCatalog.
    if (cacheExpirationIntervalMs == 0) {
      this.cacheEnabled = false;
    }

    Catalog catalog = buildIcebergCatalog(name, options);

    this.catalogName = name;
    SparkSession sparkSession = SparkSession.active();
    this.useTimestampsWithoutZone =
        SparkUtil.useTimestampWithoutZoneInNewTables(sparkSession.conf());
    this.tables =
        new HadoopTables(SparkUtil.hadoopConfCatalogOverrides(SparkSession.active(), name));
    this.icebergCatalog =
        cacheEnabled ? CachingCatalog.wrap(catalog, cacheExpirationIntervalMs) : catalog;
    if (catalog instanceof SupportsNamespaces) {
      this.asNamespaceCatalog = (SupportsNamespaces) catalog;
      if (options.containsKey("default-namespace")) {
        this.defaultNamespace =
            Splitter.on('.').splitToList(options.get("default-namespace")).toArray(new String[0]);
      }
    }

    EnvironmentContext.put(EnvironmentContext.ENGINE_NAME, "spark");
    EnvironmentContext.put(
        EnvironmentContext.ENGINE_VERSION, sparkSession.sparkContext().version());
    EnvironmentContext.put(CatalogProperties.APP_ID, sparkSession.sparkContext().applicationId());
  }

  @Override
  public String name() {
    return catalogName;
  }

  private static void commitChanges(
      org.apache.iceberg.Table table,
      SetProperty setLocation,
      SetProperty setSnapshotId,
      SetProperty pickSnapshotId,
      List<TableChange> propertyChanges,
      List<TableChange> schemaChanges) {
    // don't allow setting the snapshot and picking a commit at the same time because order is
    // ambiguous and choosing
    // one order leads to different results
    Preconditions.checkArgument(
        setSnapshotId == null || pickSnapshotId == null,
        "Cannot set the current the current snapshot ID and cherry-pick snapshot changes");

    if (setSnapshotId != null) {
      long newSnapshotId = Long.parseLong(setSnapshotId.value());
      table.manageSnapshots().setCurrentSnapshot(newSnapshotId).commit();
    }

    // if updating the table snapshot, perform that update first in case it fails
    if (pickSnapshotId != null) {
      long newSnapshotId = Long.parseLong(pickSnapshotId.value());
      table.manageSnapshots().cherrypick(newSnapshotId).commit();
    }

    Transaction transaction = table.newTransaction();

    if (setLocation != null) {
      transaction.updateLocation().setLocation(setLocation.value()).commit();
    }

    if (!propertyChanges.isEmpty()) {
      Spark3Util.applyPropertyChanges(transaction.updateProperties(), propertyChanges).commit();
    }

    if (!schemaChanges.isEmpty()) {
      Spark3Util.applySchemaChanges(transaction.updateSchema(), schemaChanges).commit();
    }

    transaction.commitTransaction();
  }

  private static boolean isPathIdentifier(Identifier ident) {
    return ident instanceof PathIdentifier;
  }

  private static void checkNotPathIdentifier(Identifier identifier, String method) {
    if (identifier instanceof PathIdentifier) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot pass path based identifier to %s method. %s is a path.", method, identifier));
    }
  }

  private Table load(Identifier ident) {
    if (isPathIdentifier(ident)) {
      return loadFromPathIdentifier((PathIdentifier) ident);
    }

    try {
      org.apache.iceberg.Table table = icebergCatalog.loadTable(buildIdentifier(ident));
      return new SparkTable(table, !cacheEnabled);

    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      if (ident.namespace().length == 0) {
        throw e;
      }

      // if the original load didn't work, try using the namespace as an identifier because
      // the original identifier may include a snapshot selector or may point to the changelog
      TableIdentifier namespaceAsIdent = buildIdentifier(namespaceToIdentifier(ident.namespace()));
      org.apache.iceberg.Table table;
      try {
        table = icebergCatalog.loadTable(namespaceAsIdent);
      } catch (Exception ignored) {
        // the namespace does not identify a table, so it cannot be a table with a snapshot selector
        // throw the original exception
        throw e;
      }

      // loading the namespace as a table worked, check the name to see if it is a valid selector
      // or if the name points to the changelog

      if (ident.name().equalsIgnoreCase(SparkChangelogTable.TABLE_NAME)) {
        return new SparkChangelogTable(table, !cacheEnabled);
      }

      Matcher at = AT_TIMESTAMP.matcher(ident.name());
      if (at.matches()) {
        long asOfTimestamp = Long.parseLong(at.group(1));
        long snapshotId = SnapshotUtil.snapshotIdAsOfTime(table, asOfTimestamp);
        return new SparkTable(table, snapshotId, !cacheEnabled);
      }

      Matcher id = SNAPSHOT_ID.matcher(ident.name());
      if (id.matches()) {
        long snapshotId = Long.parseLong(id.group(1));
        return new SparkTable(table, snapshotId, !cacheEnabled);
      }

      // the name wasn't a valid snapshot selector and did not point to the changelog
      // throw the original exception
      throw e;
    }
  }

  private Pair<String, List<String>> parseLocationString(String location) {
    int hashIndex = location.lastIndexOf('#');
    if (hashIndex != -1 && !location.endsWith("#")) {
      String baseLocation = location.substring(0, hashIndex);
      List<String> metadata = COMMA.splitToList(location.substring(hashIndex + 1));
      return Pair.of(baseLocation, metadata);
    } else {
      return Pair.of(location, ImmutableList.of());
    }
  }

  @SuppressWarnings("CyclomaticComplexity")
  private Table loadFromPathIdentifier(PathIdentifier ident) {
    Pair<String, List<String>> parsed = parseLocationString(ident.location());

    String metadataTableName = null;
    Long asOfTimestamp = null;
    Long snapshotId = null;
    boolean isChangelog = false;

    for (String meta : parsed.second()) {
      if (meta.equalsIgnoreCase(SparkChangelogTable.TABLE_NAME)) {
        isChangelog = true;
        continue;
      }

      if (MetadataTableType.from(meta) != null) {
        metadataTableName = meta;
        continue;
      }

      Matcher at = AT_TIMESTAMP.matcher(meta);
      if (at.matches()) {
        asOfTimestamp = Long.parseLong(at.group(1));
        continue;
      }

      Matcher id = SNAPSHOT_ID.matcher(meta);
      if (id.matches()) {
        snapshotId = Long.parseLong(id.group(1));
      }
    }

    Preconditions.checkArgument(
        asOfTimestamp == null || snapshotId == null,
        "Cannot specify both snapshot-id and as-of-timestamp: %s",
        ident.location());

    Preconditions.checkArgument(
        !isChangelog || (snapshotId == null && asOfTimestamp == null),
        "Cannot specify snapshot-id and as-of-timestamp for changelogs");

    org.apache.iceberg.Table table =
        tables.load(parsed.first() + (metadataTableName != null ? "#" + metadataTableName : ""));

    if (isChangelog) {
      return new SparkChangelogTable(table, !cacheEnabled);

    } else if (asOfTimestamp != null) {
      long snapshotIdAsOfTime = SnapshotUtil.snapshotIdAsOfTime(table, asOfTimestamp);
      return new SparkTable(table, snapshotIdAsOfTime, !cacheEnabled);

    } else {
      return new SparkTable(table, snapshotId, !cacheEnabled);
    }
  }

  private Identifier namespaceToIdentifier(String[] namespace) {
    Preconditions.checkArgument(
        namespace.length > 0, "Cannot convert empty namespace to identifier");
    String[] ns = Arrays.copyOf(namespace, namespace.length - 1);
    String name = namespace[ns.length];
    return Identifier.of(ns, name);
  }

  private Catalog.TableBuilder newBuilder(Identifier ident, Schema schema) {
    return isPathIdentifier(ident)
        ? tables.buildTable(((PathIdentifier) ident).location(), schema)
        : icebergCatalog.buildTable(buildIdentifier(ident), schema);
  }

  @Override
  public Catalog icebergCatalog() {
    return icebergCatalog;
  }
}
