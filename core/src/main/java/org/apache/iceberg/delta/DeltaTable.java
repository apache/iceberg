package org.apache.iceberg.delta;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.*;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import java.util.List;
import java.util.Map;

public class DeltaTable implements Table {

    private final TableOperations ops;
    private final Table table;

    public DeltaTable(TableOperations ops, Table table) {
        this.ops = ops;
        this.table = table;
    }

    @Override
    public void refresh() {
        table.refresh();
    }

    @Override
    public TableScan newScan() {
        return null;
    }

    @Override
    public Schema schema() {
        return ops.current().pkSpec().getDeltaSchema();
    }

    @Override
    public PartitionSpec spec() {
        return table.spec();
    }

    @Override
    public PrimaryKeySpec pkSpec() {
        return table.pkSpec();
    }

    @Override
    public Map<Integer, PartitionSpec> specs() {
        return table.specs();
    }

    @Override
    public Map<String, String> properties() {
        return ImmutableMap.of();
    }

    @Override
    public String location() {
        return ops.current().deltaLocation();
    }

    @Override
    public Snapshot currentSnapshot() {
        return table.currentSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return table.snapshot(snapshotId);
    }

    @Override
    public Iterable<Snapshot> snapshots() {
        return table.snapshots();
    }

    @Override
    public List<HistoryEntry> history() {
        return table.history();
    }

    @Override
    public UpdateSchema updateSchema() {
        throw new UnsupportedOperationException("Cannot update the schema of a delta table");
    }

    @Override
    public UpdateProperties updateProperties() {
        throw new UnsupportedOperationException("Cannot update the properties of a delta table");
    }

    @Override
    public UpdateLocation updateLocation() {
        throw new UnsupportedOperationException("Cannot update the location of a delta table");
    }

    @Override
    public AppendFiles newAppend() {
        throw new UnsupportedOperationException("Cannot append to a delta table");
    }

    @Override
    public RewriteFiles newRewrite() {
        throw new UnsupportedOperationException("Cannot rewrite in a delta table");
    }

    @Override
    public RewriteManifests rewriteManifests() {
        throw new UnsupportedOperationException("Cannot rewrite manifests in a metadata table");
    }

    @Override
    public OverwriteFiles newOverwrite() {
        throw new UnsupportedOperationException("Cannot overwrite in a delta table");
    }

    @Override
    public ReplacePartitions newReplacePartitions() {
        throw new UnsupportedOperationException("Cannot replace partitions in a delta table");
    }

    @Override
    public DeleteFiles newDelete() {
        throw new UnsupportedOperationException("Cannot delete from a delta table");
    }

    @Override
    public AppendDeltaFiles newDeltaAppend() {
        throw new UnsupportedOperationException("Cannot append to a delta table");
    }

    @Override
    public ExpireSnapshots expireSnapshots() {
        throw new UnsupportedOperationException("Cannot expire snapshots from a delta table");
    }

    @Override
    public Rollback rollback() {
        throw new UnsupportedOperationException("Cannot roll back a delta table");
    }

    @Override
    public ManageSnapshots manageSnapshots() {
        throw new UnsupportedOperationException("Cannot manage snapshots in a delta table");
    }

    @Override
    public Transaction newTransaction() {
        throw new UnsupportedOperationException("Cannot create transactions for a delta table");
    }

    @Override
    public FileIO io() {
        return table.io();
    }

    @Override
    public EncryptionManager encryption() {
        return table.encryption();
    }

    @Override
    public LocationProvider locationProvider() {
        return table.locationProvider();
    }

    @Override
    public String toString() {
        return table.toString() + "." + TableMetadata.DELTA_FOLDER_NAME;
    }
}
