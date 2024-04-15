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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.Before;

public abstract class MetadataTableFiltersCommon extends TableTestBase {

  protected static final Set<MetadataTableType> aggFileTables =
      Sets.newHashSet(
          MetadataTableType.ALL_DATA_FILES,
          MetadataTableType.ALL_DATA_FILES,
          MetadataTableType.ALL_FILES,
          MetadataTableType.ALL_ENTRIES);

  @Parameters(name = "formatVersion = {0}, table_type = {1}")
  public static List<Object> parameters() {
    return Arrays.asList(
        new Object[] {1, MetadataTableType.DATA_FILES},
        new Object[] {2, MetadataTableType.DATA_FILES},
        new Object[] {2, MetadataTableType.DELETE_FILES},
        new Object[] {1, MetadataTableType.FILES},
        new Object[] {2, MetadataTableType.FILES},
        new Object[] {1, MetadataTableType.ALL_DATA_FILES},
        new Object[] {2, MetadataTableType.ALL_DATA_FILES},
        new Object[] {2, MetadataTableType.ALL_DELETE_FILES},
        new Object[] {1, MetadataTableType.ALL_FILES},
        new Object[] {2, MetadataTableType.ALL_FILES},
        new Object[] {1, MetadataTableType.ENTRIES},
        new Object[] {2, MetadataTableType.ENTRIES},
        new Object[] {1, MetadataTableType.ALL_ENTRIES},
        new Object[] {2, MetadataTableType.ALL_ENTRIES});
  }

  @Parameter(index = 1)
  protected MetadataTableType type;

  protected MetadataTableFiltersCommon(MetadataTableType type, int formatVersion) {
    super(formatVersion);
    this.type = type;
  }

  /** @return a basic expression that always evaluates to true, to test AND logic */
  protected Expression dummyExpression() {
    switch (type) {
      case FILES:
      case DATA_FILES:
      case DELETE_FILES:
      case ALL_DATA_FILES:
      case ALL_DELETE_FILES:
      case ALL_FILES:
        return Expressions.greaterThan("record_count", 0);
      case ENTRIES:
      case ALL_ENTRIES:
        return Expressions.greaterThan("data_file.record_count", 0);
      default:
        throw new IllegalArgumentException("Unsupported metadata table type:" + type);
    }
  }

  @Before
  @Override
  public void setupTable() throws Exception {
    super.setupTable();
    table.updateProperties().set(TableProperties.MANIFEST_MERGE_ENABLED, "false").commit();
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_C).commit();
    table.newFastAppend().appendFile(FILE_D).commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    if (formatVersion == 2) {
      table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
      table.newRowDelta().addDeletes(FILE_B_DELETES).commit();
      table.newRowDelta().addDeletes(FILE_C2_DELETES).commit();
      table.newRowDelta().addDeletes(FILE_D2_DELETES).commit();
    }

    if (isAggFileTable(type)) {
      // Clear all files from current snapshot to test whether 'all' Files tables scans previous
      // files
      table
          .newDelete()
          .deleteFromRowFilter(Expressions.alwaysTrue())
          .commit(); // Moves file entries to DELETED state
      table
          .newDelete()
          .deleteFromRowFilter(Expressions.alwaysTrue())
          .commit(); // Removes all entries
      assertThat(table.currentSnapshot().allManifests(table.io())).isEmpty();
    }
  }

  protected String partitionColumn(String colName) {
    switch (type) {
      case FILES:
      case DATA_FILES:
      case DELETE_FILES:
      case ALL_DATA_FILES:
      case ALL_DELETE_FILES:
      case ALL_FILES:
        return String.format("partition.%s", colName);
      case ENTRIES:
      case ALL_ENTRIES:
        return String.format("data_file.partition.%s", colName);
      default:
        throw new IllegalArgumentException("Unsupported metadata table type:" + type);
    }
  }

  protected boolean isAggFileTable(MetadataTableType tableType) {
    return aggFileTables.contains(tableType);
  }

  protected boolean manifestHasPartition(ManifestFile mf, int partValue) {
    int lower =
        Conversions.fromByteBuffer(Types.IntegerType.get(), mf.partitions().get(0).lowerBound());
    int upper =
        Conversions.fromByteBuffer(Types.IntegerType.get(), mf.partitions().get(0).upperBound());
    return (lower <= partValue) && (upper >= partValue);
  }

  protected ManifestFile manifest(FileScanTask task) {
    if (task instanceof BaseFilesTable.ManifestReadTask) {
      return ((BaseFilesTable.ManifestReadTask) task).manifest();
    } else if (task instanceof BaseEntriesTable.ManifestReadTask) {
      return ((BaseEntriesTable.ManifestReadTask) task).manifest();
    } else {
      throw new IllegalArgumentException(
          "Unexpected task type: " + task.getClass().getCanonicalName());
    }
  }

  protected Table createMetadataTable() {
    switch (type) {
      case FILES:
        return new FilesTable(table);
      case DATA_FILES:
        return new DataFilesTable(table);
      case DELETE_FILES:
        return new DeleteFilesTable(table);
      case ALL_DATA_FILES:
        return new AllDataFilesTable(table);
      case ALL_DELETE_FILES:
        return new AllDeleteFilesTable(table);
      case ALL_FILES:
        return new AllFilesTable(table);
      case ENTRIES:
        return new ManifestEntriesTable(table);
      case ALL_ENTRIES:
        return new AllEntriesTable(table);
      default:
        throw new IllegalArgumentException("Unsupported metadata table type:" + type);
    }
  }

  protected void validateFileScanTasks(
      CloseableIterable<FileScanTask> fileScanTasks, int partValue) {
    assertThat(fileScanTasks)
        .as("File scan tasks do not include correct file")
        .anyMatch(t -> manifestHasPartition(manifest(t), partValue));
  }

  protected int expectedScanTaskCount(int partitions) {
    switch (type) {
      case FILES:
      case ENTRIES:
        if (formatVersion == 1) {
          return partitions;
        } else {
          return partitions * 2; // Delete File and Data File per partition
        }
      case DATA_FILES:
      case DELETE_FILES:
      case ALL_DELETE_FILES:
        return partitions;
      case ALL_DATA_FILES:
        return partitions * 2; // ScanTask for Data Manifest in DELETED and ADDED states
      case ALL_FILES:
      case ALL_ENTRIES:
        if (formatVersion == 1) {
          return partitions * 2; // ScanTask for Data Manifest in DELETED and ADDED states
        } else {
          return partitions * 4; // ScanTask for Delete and Data File in DELETED and ADDED states
        }
      default:
        throw new IllegalArgumentException("Unsupported metadata table type:" + type);
    }
  }
}
