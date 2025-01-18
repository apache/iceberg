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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewOperations;

public abstract class BaseViewMetadataTable extends BaseReadOnlyTable implements Serializable {

    //TODO: refactor and share same code as BaseMetadataTable
    private final PartitionSpec spec = PartitionSpec.unpartitioned();

    private final SortOrder sortOrder = SortOrder.unsorted();
    private final BaseView view;
    private final String name;
    private final UUID uuid;

    protected BaseViewMetadataTable(View view, String name) {
        super("metadata");
        Preconditions.checkArgument(
                view instanceof BaseView, "Cannot create metadata table for view: %s", view);
        this.view = (BaseView) view;
        this.name = name;
        this.uuid = UUID.randomUUID();
    }

    protected ViewOperations operations() {
        return view.operations();
    }

    @Override
    public void refresh() {
        view.operations().refresh();
    }

    @Override
    public PartitionSpec spec() {
        return spec;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public FileIO io() {
        return null;
    }

    @Override
    public String location() {
        return view.location();
    }

    @Override
    public EncryptionManager encryption() {
        return null;
    }

    @Override
    public LocationProvider locationProvider() {
        return null;
    }

    @Override
    public Map<Integer, Schema> schemas() {
        return ImmutableMap.of(TableMetadata.INITIAL_SCHEMA_ID, schema());
    }


    @Override
    public Map<Integer, PartitionSpec> specs() {
        return ImmutableMap.of(spec.specId(), spec);
    }

    @Override
    public SortOrder sortOrder() {
        return sortOrder;
    }

    @Override
    public Map<Integer, SortOrder> sortOrders() {
        return ImmutableMap.of(sortOrder.orderId(), sortOrder);
    }


    @Override
    public Map<String, String> properties() {
        return ImmutableMap.of();
    }

    @Override
    public Snapshot currentSnapshot() {
        return null;
    }


    @Override
    public Iterable<Snapshot> snapshots() {
        return ImmutableList.of();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return null;
    }

    @Override
    public List<HistoryEntry> history() {
        return ImmutableList.of();
    }

    @Override
    public List<StatisticsFile> statisticsFiles() {
        return ImmutableList.of();
    }

    @Override
    public List<PartitionStatisticsFile> partitionStatisticsFiles() {
        return ImmutableList.of();
    }

    @Override
    public Map<String, SnapshotRef> refs() {
        return ImmutableMap.of();
    }

    @Override
    public UUID uuid() {
        return uuid;
    }

    @Override
    public String toString() {
        return name();
    }

    final Object writeReplace() {
        return SerializableTable.copyOf(this);
    }

}
