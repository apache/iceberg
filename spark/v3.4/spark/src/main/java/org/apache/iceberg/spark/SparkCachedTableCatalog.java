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
public class SparkCachedTableCatalog implements TableCatalog {

  private static final String CLASS_NAME = SparkCachedTableCatalog.class.getName();
  private static final Splitter COMMA = Splitter.on(",");
  private static final Pattern AT_TIMESTAMP = Pattern.compile("at_timestamp_(\\d+)");
  private static final Pattern SNAPSHOT_ID = Pattern.compile("snapshot_id_(\\d+)");
  private static final Pattern BRANCH = Pattern.compile("branch_(.*)");
  private static final Pattern TAG = Pattern.compile("tag_(.*)");

  private static final SparkTableCache TABLE_CACHE = SparkTableCache.get();

  private String name = null;

  @Override
  public Identifier[] listTables(String[] namespace) {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support listing tables");
  }

  @Override
  public SparkTable loadTable(Identifier ident) throws NoSuchTableException {
    Pair<Table, Long> table = load(ident);
    return new SparkTable(table.first(), table.second(), false /* refresh eagerly */);
  }

  @Override
  public SparkTable loadTable(Identifier ident, String version) throws NoSuchTableException {
    Pair<Table, Long> table = load(ident);
    Preconditions.checkArgument(
        table.second() == null, "Cannot time travel based on both table identifier and AS OF");
    return new SparkTable(table.first(), Long.parseLong(version), false /* refresh eagerly */);
  }

  @Override
  public SparkTable loadTable(Identifier ident, long timestampMicros) throws NoSuchTableException {
    Pair<Table, Long> table = load(ident);
    Preconditions.checkArgument(
        table.second() == null, "Cannot time travel based on both table identifier and AS OF");
    // Spark passes microseconds but Iceberg uses milliseconds for snapshots
    long timestampMillis = TimeUnit.MICROSECONDS.toMillis(timestampMicros);
    long snapshotId = SnapshotUtil.snapshotIdAsOfTime(table.first(), timestampMillis);
    return new SparkTable(table.first(), snapshotId, false /* refresh eagerly */);
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

  private Pair<Table, Long> load(Identifier ident) throws NoSuchTableException {
    Preconditions.checkArgument(
        ident.namespace().length == 0, CLASS_NAME + " does not support namespaces");

    Pair<String, List<String>> parsedIdent = parseIdent(ident);
    String key = parsedIdent.first();
    List<String> metadata = parsedIdent.second();

    Long asOfTimestamp = null;
    Long snapshotId = null;
    String branch = null;
    String tag = null;
    for (String meta : metadata) {
      Matcher timeBasedMatcher = AT_TIMESTAMP.matcher(meta);
      if (timeBasedMatcher.matches()) {
        asOfTimestamp = Long.parseLong(timeBasedMatcher.group(1));
        continue;
      }

      Matcher snapshotBasedMatcher = SNAPSHOT_ID.matcher(meta);
      if (snapshotBasedMatcher.matches()) {
        snapshotId = Long.parseLong(snapshotBasedMatcher.group(1));
        continue;
      }

      Matcher branchBasedMatcher = BRANCH.matcher(meta);
      if (branchBasedMatcher.matches()) {
        branch = branchBasedMatcher.group(1);
        continue;
      }

      Matcher tagBasedMatcher = TAG.matcher(meta);
      if (tagBasedMatcher.matches()) {
        tag = tagBasedMatcher.group(1);
      }
    }

    Preconditions.checkArgument(
        Stream.of(snapshotId, asOfTimestamp, branch, tag).filter(Objects::nonNull).count() <= 1,
        "Can specify only one of snapshot-id (%s), as-of-timestamp (%s), branch (%s), tag (%s)",
        snapshotId,
        asOfTimestamp,
        branch,
        tag);

    Table table = TABLE_CACHE.get(key);

    if (table == null) {
      throw new NoSuchTableException(ident);
    }

    if (snapshotId != null) {
      return Pair.of(table, snapshotId);
    } else if (asOfTimestamp != null) {
      return Pair.of(table, SnapshotUtil.snapshotIdAsOfTime(table, asOfTimestamp));
    } else if (branch != null) {
      Snapshot branchSnapshot = table.snapshot(branch);
      Preconditions.checkArgument(
          branchSnapshot != null, "Cannot find snapshot associated with branch name: %s", branch);
      return Pair.of(table, branchSnapshot.snapshotId());
    } else if (tag != null) {
      Snapshot tagSnapshot = table.snapshot(tag);
      Preconditions.checkArgument(
          tagSnapshot != null, "Cannot find snapshot associated with tag name: %s", tag);
      return Pair.of(table, tagSnapshot.snapshotId());
    } else {
      return Pair.of(table, null);
    }
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
