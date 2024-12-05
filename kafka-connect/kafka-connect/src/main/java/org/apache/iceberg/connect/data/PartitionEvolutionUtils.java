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

package org.apache.iceberg.connect.data;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.evolution.AddPartitionUpdater;
import org.apache.iceberg.connect.data.evolution.PartitionUpdater;
import org.apache.iceberg.connect.data.evolution.RemovePartitionUpdater;
import org.apache.iceberg.transforms.Transform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PartitionEvolutionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionEvolutionUtils.class);

    public static void checkAndEvolvePartition(Table table, IcebergSinkConfig config) {
        List<String> removePartitionFields = config.tableConfig(table.name()).removePartitionBy();
        List<String> addPartitionFields = config.tableConfig(table.name()).addPartitionBy();

        removeCommonElements(removePartitionFields, addPartitionFields);

        if (addPartitionFields.isEmpty() && removePartitionFields.isEmpty()) {
            LOG.info("Nothing to add or remove for job = {}", config.connectorName());
            return;
        }

        UpdatePartitionSpec updateSpec = table.updateSpec();
        boolean hasUpdates = false;

        if (!removePartitionFields.isEmpty()) {
            hasUpdates |= processPartitionUpdate(new RemovePartitionUpdater(table, updateSpec), removePartitionFields, table);
        }

        if (!addPartitionFields.isEmpty()) {
            hasUpdates |= processPartitionUpdate(new AddPartitionUpdater(table, updateSpec), addPartitionFields, table);
        }

        if (hasUpdates) {
            commitPartitionUpdate(updateSpec, config);
        }
    }

    private static boolean processPartitionUpdate(PartitionUpdater updater, List<String> partitionFields, Table table) {
        try {
            PartitionSpec spec = SchemaUtils.createPartitionSpec(table.schema(), partitionFields);
            return updater.update(spec);
        } catch (Exception ex) {
            LOG.warn("Failed to build partition spec for fields: {}", partitionFields, ex);
            return false;
        }
    }

    private static void commitPartitionUpdate(UpdatePartitionSpec updateSpec, IcebergSinkConfig config) {
        try {
            updateSpec.commit();
        } catch (Exception ex) {
            LOG.warn("Exception while committing partition update for job = {}. Continuing...", config.connectorName(), ex);
        }
    }

    private static <T> void removeCommonElements(List<T> list1, List<T> list2) {
        Set<T> commonElements = new HashSet<>(list1);
        commonElements.retainAll(list2);
        list1.removeAll(commonElements);
        list2.removeAll(commonElements);
    }

    public static boolean isIdentityTransform(Transform<?, ?> transform) {
        return transform.isIdentity();
    }
}