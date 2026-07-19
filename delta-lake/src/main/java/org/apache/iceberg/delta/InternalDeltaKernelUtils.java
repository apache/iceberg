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
package org.apache.iceberg.delta;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A temporary utility class to encapsulate the usage of internal Delta Kernel APIs.
 *
 * <p>This class is required because certain metadata and action APIs are not yet exposed in the
 * public Delta Kernel API. Once the Delta Kernel public API is improved to support these use cases,
 * this class will be deleted.
 */
class InternalDeltaKernelUtils {

  private InternalDeltaKernelUtils() {}

  static long earliestRecreatableCommit(Engine engine, String tableLocation)
      throws TableNotFoundException {
    return DeltaHistoryManager.getEarliestRecreatableCommit(
        engine, new Path(tableLocation, "_delta_log"));
  }

  static CloseableIterator<ColumnarBatch> changes(
      Table table, Engine engine, long startVersion, long endVersion) {
    return ((TableImpl) table)
        .getChanges(
            engine,
            startVersion,
            endVersion,
            Arrays.stream(DeltaLogActionUtils.DeltaAction.values()).collect(Collectors.toSet()));
  }

  static Map<String, String> metadataConfiguration(Snapshot snapshot) {
    return ((SnapshotImpl) snapshot).getMetadata().getConfiguration();
  }

  static DeltaAddFile toAddFile(Row row) {
    return new DeltaAddFile(row);
  }

  static DeltaRemoveFile toRemoveFile(Row row) {
    return new DeltaRemoveFile(row);
  }

  static long[] readDeltaDVPositions(Engine engine, String tablePath, DeltaAddFile addFile) {
    DeletionVectorDescriptor descriptor = addFile.deletionVector();
    if (descriptor == null) {
      return new long[0];
    }
    Tuple2<DeletionVectorDescriptor, RoaringBitmapArray> tuple =
        DeletionVectorUtils.loadNewDvAndBitmap(engine, tablePath, descriptor);
    return tuple._2.toArray();
  }

  static void assertSnapshotImpl(Snapshot latestSnapshot) {
    if (!(latestSnapshot instanceof SnapshotImpl)) {
      throw new IllegalStateException(
          "Unsupported impl of delta Snapshot: " + latestSnapshot.getClass());
    }
  }

  /**
   * A wrapper class around the Delta Lake "add" action Row.
   *
   * <p>The field names (such as "add", "path", "size", "partitionValues", and "deletionVector") are
   * defined by the Delta Transaction Log Protocol and are guaranteed to remain stable. Therefore,
   * it is safe to access them directly by name from the underlying Row.
   *
   * <p>For more information see {@link io.delta.kernel.internal.actions.AddFile}.
   */
  static class DeltaAddFile {
    private final Row addFileRow;

    DeltaAddFile(Row row) {
      this.addFileRow = row.getStruct(row.getSchema().indexOf("add"));
    }

    public String path() {
      return addFileRow.getString(addFileRow.getSchema().indexOf("path"));
    }

    public long size() {
      return addFileRow.getLong(addFileRow.getSchema().indexOf("size"));
    }

    public boolean hasDeletionVector() {
      int dvIndex = addFileRow.getSchema().indexOf("deletionVector");
      return dvIndex >= 0 && !addFileRow.isNullAt(dvIndex);
    }

    public Map<String, String> partitionValues() {
      return VectorUtils.toJavaMap(
          addFileRow.getMap(addFileRow.getSchema().indexOf("partitionValues")));
    }

    public DeletionVectorDescriptor deletionVector() {
      int dvIndex = addFileRow.getSchema().indexOf("deletionVector");
      if (dvIndex < 0 || addFileRow.isNullAt(dvIndex)) {
        return null;
      }
      return DeletionVectorDescriptor.fromRow(addFileRow.getStruct(dvIndex));
    }
  }

  /**
   * A wrapper class around the Delta Lake "remove" action Row.
   *
   * <p>The field names (such as "remove", "path", and "partitionValues") are defined by the Delta
   * Transaction Log Protocol and are guaranteed to remain stable. Therefore, it is safe to access
   * them directly by name from the underlying Row.
   *
   * <p>For more information see {@link io.delta.kernel.internal.actions.RemoveFile}.
   */
  static class DeltaRemoveFile {
    private final Row removeFileRow;

    DeltaRemoveFile(Row row) {
      this.removeFileRow = row.getStruct(row.getSchema().indexOf("remove"));
    }

    public String path() {
      return removeFileRow.getString(removeFileRow.getSchema().indexOf("path"));
    }

    public Map<String, String> partitionValues() {
      int partitionValuesIndex = removeFileRow.getSchema().indexOf("partitionValues");
      if (partitionValuesIndex < 0 || removeFileRow.isNullAt(partitionValuesIndex)) {
        return Map.of();
      }
      return VectorUtils.toJavaMap(removeFileRow.getMap(partitionValuesIndex));
    }
  }
}
