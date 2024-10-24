/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.connect.data.evolution;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.connect.data.PartitionEvolutionUtils;
import org.apache.iceberg.expressions.Expressions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddPartitionUpdater implements PartitionUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(AddPartitionUpdater.class);

    private final Table table;
    private final UpdatePartitionSpec updateSpec;

    public AddPartitionUpdater(Table table, UpdatePartitionSpec updateSpec) {
        this.table = table;
        this.updateSpec = updateSpec;
    }

    @Override
    public boolean update(PartitionSpec spec) {
        boolean hasUpdates = false;
        Set<String> currentPartitionColumns = getCurrentPartitionColumns(table);
        LOG.info("currentPartitionColumns while adding = {}", currentPartitionColumns);
        for (PartitionField field : spec.fields()) {
            if (!currentPartitionColumns.contains(field.name())) {
                LOG.info("field.name = {} not found in current partition fields = {}, hence will try to add", field.name(), currentPartitionColumns);
                hasUpdates = true;
                applyTransformation(field, Arrays.stream(field.name().split("_")).findFirst().get());
            }
        }
        return hasUpdates;
    }

    private void applyTransformation(PartitionField field, String columnName) {
        if (PartitionEvolutionUtils.isIdentityTransform(field.transform())) {
            updateSpec.addField(columnName);
        } else {
            updateSpec.addField(Expressions.transform(columnName, field.transform()));
        }
    }

    private Set<String> getCurrentPartitionColumns(Table table) {
        return table.spec().fields().stream()
                .map(PartitionField::name)
                .collect(Collectors.toSet());
    }
}