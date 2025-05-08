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
package org.apache.iceberg.actions;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.hadoop.HiddenPathFilter;

/**
 * A {@link PathFilter} that filters out hidden path, but does not filter out paths that would be
 * marked as hidden by {@link HiddenPathFilter} due to a partition field that starts with one of the
 * characters that indicate a hidden path.
 */
public class PartitionAwareHiddenPathFilter implements PathFilter, Serializable {

  private final Set<String> hiddenPathPartitionNames;

  private PartitionAwareHiddenPathFilter(Set<String> hiddenPathPartitionNames) {
    this.hiddenPathPartitionNames = hiddenPathPartitionNames;
  }

  @Override
  public boolean accept(Path path) {
    return isHiddenPartitionPath(path) || HiddenPathFilter.get().accept(path);
  }

  private boolean isHiddenPartitionPath(Path path) {
    return hiddenPathPartitionNames.stream().anyMatch(path.getName()::startsWith);
  }

  public static PathFilter forSpecs(Map<Integer, PartitionSpec> specs) {
    if (specs == null) {
      return HiddenPathFilter.get();
    }

    Set<String> partitionNames =
        specs.values().stream()
            .map(PartitionSpec::fields)
            .flatMap(List::stream)
            .filter(field -> field.name().startsWith("_") || field.name().startsWith("."))
            .map(field -> field.name() + "=")
            .collect(Collectors.toSet());

    if (partitionNames.isEmpty()) {
      return HiddenPathFilter.get();
    } else {
      return new PartitionAwareHiddenPathFilter(partitionNames);
    }
  }
}
