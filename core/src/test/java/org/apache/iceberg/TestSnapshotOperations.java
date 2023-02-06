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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.SerializableSupplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class TestSnapshotOperations extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public TestSnapshotOperations(int formatVersion) {
    super(formatVersion);
  }

  private Snapshot currentSnapshot;
  private List<Snapshot> allSnapshots;
  private SerializableSupplier<List<Snapshot>> snapshotSupplier;

  @Before
  public void before() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    this.currentSnapshot = table.currentSnapshot();
    this.allSnapshots = Lists.newArrayList(table.snapshots());

    // Anonymous class is required for proper mocking as opposed to lambda
    this.snapshotSupplier =
        new SerializableSupplier<List<Snapshot>>() {
          @Override
          public List<Snapshot> get() {
            return allSnapshots;
          }
        };
  }

  @Test
  public void testSnapshotsLoadBehavior() {
    SerializableSupplier<List<Snapshot>> snapshotsSupplierMock = Mockito.spy(snapshotSupplier);

    List<Snapshot> initialSnapshots =
        currentSnapshot != null
            ? Collections.singletonList(currentSnapshot)
            : Collections.emptyList();

    SnapshotOperations snapshotOperations =
        SnapshotOperations.buildFromEmpty()
            .snapshots(initialSnapshots)
            .snapshotsSupplier(snapshotsSupplierMock)
            .build();

    snapshotOperations.snapshots();
    snapshotOperations.snapshots();
    snapshotOperations.snapshots();

    // Verify that the snapshot supplier only gets called once even with repeated calls to load
    // snapshots
    verify(snapshotsSupplierMock, times(1)).get();
  }

  @Test
  public void testUnloadedSnapshotReference() {
    SerializableSupplier<List<Snapshot>> snapshotsSupplierMock = Mockito.spy(snapshotSupplier);

    List<Snapshot> initialSnapshots =
        currentSnapshot != null
            ? Collections.singletonList(currentSnapshot)
            : Collections.emptyList();

    SnapshotOperations snapshotOperations =
        SnapshotOperations.buildFromEmpty()
            .snapshots(initialSnapshots)
            .snapshotsSupplier(snapshotsSupplierMock)
            .build();

    // Ensure all snapshots are reachable
    allSnapshots.forEach(
        snapshot ->
            assertThat(snapshotOperations.snapshot(snapshot.snapshotId())).isEqualTo(snapshot));

    // Verify that loading was called while checking snapshots
    verify(snapshotsSupplierMock, times(1)).get();
  }

  @Test
  public void testCurrentSnapshot() {
    SerializableSupplier<List<Snapshot>> snapshotsSupplierMock = Mockito.spy(snapshotSupplier);

    SnapshotOperations snapshotOperations =
        SnapshotOperations.buildFromEmpty()
            .add(currentSnapshot)
            .snapshotsSupplier(snapshotsSupplierMock)
            .addRef(
                SnapshotRef.MAIN_BRANCH,
                SnapshotRef.branchBuilder(currentSnapshot.snapshotId()).build())
            .build();

    TableMetadata testMetadata =
        TableMetadata.buildFrom(table.ops().current())
            .setSnapshotOperations(snapshotOperations)
            .build();

    Table t1 = new BaseTable(new MetadataTableOperations(table.io(), testMetadata), "temp");

    // Loading the current snapshot should not invoke the supplier
    t1.currentSnapshot();

    // Performing a table scan should not invoke supplier
    t1.newScan().planFiles().forEach(t -> {});

    // Performing a table scan on main should not invoke supplier
    t1.newScan().useRef(SnapshotRef.MAIN_BRANCH).planFiles().forEach(t -> {});

    verify(snapshotsSupplierMock, times(0)).get();
  }

  @Test
  public void testSnapshotOperationsSerialization() throws IOException, ClassNotFoundException {
    final List<Snapshot> allSnapshots = Lists.newArrayList(table.snapshots());

    SnapshotOperations snapshotOperations =
        SnapshotOperations.buildFromEmpty()
            .add(currentSnapshot)
            .snapshotsSupplier(() -> allSnapshots)
            .build();

    SnapshotOperations result = TestHelpers.roundTripSerialize(snapshotOperations);

    assertThat(result).isNotNull();

    assertThat(table.snapshots()).containsExactlyElementsOf(snapshotOperations.snapshots());
  }

  private static class MetadataTableOperations implements TableOperations {
    private FileIO io;
    private TableMetadata currentMetadata;

    public MetadataTableOperations(FileIO io, TableMetadata currentMetadata) {
      this.io = io;
      this.currentMetadata = currentMetadata;
    }

    @Override
    public TableMetadata current() {
      return currentMetadata;
    }

    @Override
    public TableMetadata refresh() {
      throw new UnsupportedOperationException("refresh not support for test ops implementation.");
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      throw new UnsupportedOperationException("commit not support for test ops implementation.");
    }

    @Override
    public FileIO io() {
      return io;
    }

    @Override
    public String metadataFileLocation(String fileName) {
      throw new UnsupportedOperationException(
          "metadataFileLocation not support for test ops implementation.");
    }

    @Override
    public LocationProvider locationProvider() {
      throw new UnsupportedOperationException(
          "locationProvider not support for test ops implementation.");
    }
  }
}
