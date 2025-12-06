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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** An internal table catalog that is capable of loading tables from a cache. */
public class SparkCachedTableCatalog implements TableCatalog, SupportsFunctions {

  private static final String CLASS_NAME = SparkCachedTableCatalog.class.getName();
  private static final Splitter COMMA = Splitter.on(",");
  private static final Pattern AT_TIMESTAMP = Pattern.compile("at_timestamp_(\\d+)");
  private static final Pattern SNAPSHOT_ID = Pattern.compile("snapshot_id_(\\d+)");
  private static final Pattern BRANCH = Pattern.compile("branch_(.*)");
  private static final Pattern TAG = Pattern.compile("tag_(.*)");
  private static final String REWRITE = "rewrite";

  private static final SparkTableCache TABLE_CACHE = SparkTableCache.get();

  private String name = null;

  @Override
  public Identifier[] listTables(String[] namespace) {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support listing tables");
  }

  @Override
  public SparkTable loadTable(Identifier ident) throws NoSuchTableException {
    return load(ident);
  }

  @Override
  public SparkTable loadTable(Identifier ident, String version) throws NoSuchTableException {
    SparkTable table = load(ident);
    Preconditions.checkArgument(
        table.snapshotId() == null, "Cannot time travel based on both table identifier and AS OF");
    return table.copyWithSnapshotId(Long.parseLong(version));
  }

  @Override
  public SparkTable loadTable(Identifier ident, long timestampMicros) throws NoSuchTableException {
    SparkTable table = load(ident);
    Preconditions.checkArgument(
        table.snapshotId() == null, "Cannot time travel based on both table identifier and AS OF");
    // Spark passes microseconds but Iceberg uses milliseconds for snapshots
    long timestampMillis = TimeUnit.MICROSECONDS.toMillis(timestampMicros);
    long snapshotId = SnapshotUtil.snapshotIdAsOfTime(table.table(), timestampMillis);
    return table.copyWithSnapshotId(snapshotId);
  }

  @Override
  public void invalidateTable(Identifier ident) {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support table invalidation");
  }

  @Override
  public SparkTable createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support creating tables");
  }

  @Override
  public SparkTable alterTable(Identifier ident, TableChange... changes) {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support altering tables");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support dropping tables");
  }

  @Override
  public boolean purgeTable(Identifier ident) throws UnsupportedOperationException {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support purging tables");
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support renaming tables");
  }

  @Override
  public void initialize(String catalogName, CaseInsensitiveStringMap options) {
    this.name = catalogName;
  }

  @Override
  public String name() {
    return name;
  }

  private SparkTable load(Identifier ident) throws NoSuchTableException {
    Preconditions.checkArgument(
        ident.namespace().length == 0, CLASS_NAME + " does not support namespaces");

    Pair<String, List<String>> parsedIdent = parseIdent(ident);
    String key = parsedIdent.first();
    TableLoadOptions options = parseLoadOptions(parsedIdent.second());

    Table table = TABLE_CACHE.get(key);

    if (table == null) {
      throw new NoSuchTableException(ident);
    }

    if (options.isTableRewrite()) {
      return new SparkTable(table, null, false, true);
    }

    if (options.snapshotId() != null) {
      return new SparkTable(table, options.snapshotId(), false);
    } else if (options.asOfTimestamp() != null) {
      return new SparkTable(
          table, SnapshotUtil.snapshotIdAsOfTime(table, options.asOfTimestamp()), false);
    } else if (options.branch() != null) {
      Snapshot branchSnapshot = table.snapshot(options.branch());
      Preconditions.checkArgument(
          branchSnapshot != null,
          "Cannot find snapshot associated with branch name: %s",
          options.branch());
      return new SparkTable(table, branchSnapshot.snapshotId(), false);
    } else if (options.tag() != null) {
      Snapshot tagSnapshot = table.snapshot(options.tag());
      Preconditions.checkArgument(
          tagSnapshot != null, "Cannot find snapshot associated with tag name: %s", options.tag());
      return new SparkTable(table, tagSnapshot.snapshotId(), false);
    } else {
      return new SparkTable(table, false);
    }
  }

  private static class TableLoadOptions {
    private Long asOfTimestamp;
    private Long snapshotId;
    private String branch;
    private String tag;
    private Boolean isTableRewrite;

    Long asOfTimestamp() {
      return asOfTimestamp;
    }

    Long snapshotId() {
      return snapshotId;
    }

    String branch() {
      return branch;
    }

    String tag() {
      return tag;
    }

    boolean isTableRewrite() {
      return Boolean.TRUE.equals(isTableRewrite);
    }
  }

  /** Extracts table load options from metadata. */
  private TableLoadOptions parseLoadOptions(List<String> metadata) {
    TableLoadOptions opts = new TableLoadOptions();
    for (String meta : metadata) {
      Matcher timeBasedMatcher = AT_TIMESTAMP.matcher(meta);
      if (timeBasedMatcher.matches()) {
        opts.asOfTimestamp = Long.parseLong(timeBasedMatcher.group(1));
        continue;
      }

      Matcher snapshotBasedMatcher = SNAPSHOT_ID.matcher(meta);
      if (snapshotBasedMatcher.matches()) {
        opts.snapshotId = Long.parseLong(snapshotBasedMatcher.group(1));
        continue;
      }

      Matcher branchBasedMatcher = BRANCH.matcher(meta);
      if (branchBasedMatcher.matches()) {
        opts.branch = branchBasedMatcher.group(1);
        continue;
      }

      Matcher tagBasedMatcher = TAG.matcher(meta);
      if (tagBasedMatcher.matches()) {
        opts.tag = tagBasedMatcher.group(1);
      }

      if (meta.equalsIgnoreCase(REWRITE)) {
        opts.isTableRewrite = true;
      }
    }

    long numberOptions =
        Stream.of(opts.snapshotId, opts.asOfTimestamp, opts.branch, opts.tag, opts.isTableRewrite)
            .filter(Objects::nonNull)
            .count();
    Preconditions.checkArgument(
        numberOptions <= 1,
        "Can specify only one of snapshot-id (%s), as-of-timestamp (%s), branch (%s), tag (%s), is-table-rewrite (%s)",
        opts.snapshotId,
        opts.asOfTimestamp,
        opts.branch,
        opts.tag,
        opts.isTableRewrite);

    return opts;
  }

  private Pair<String, List<String>> parseIdent(Identifier ident) {
    int hashIndex = ident.name().lastIndexOf('#');
    if (hashIndex != -1 && !ident.name().endsWith("#")) {
      String key = ident.name().substring(0, hashIndex);
      List<String> metadata = COMMA.splitToList(ident.name().substring(hashIndex + 1));
      return Pair.of(key, metadata);
    } else {
      return Pair.of(ident.name(), ImmutableList.of());
    }
  }
}
