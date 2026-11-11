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
package org.apache.iceberg.tornadovm.join;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceValue;
import org.apache.iceberg.tornadovm.GroupedAggregate;
import org.apache.iceberg.types.Types;

/**
 * Star joins on the device: a device scan of a fact table, joined to dimensions given as arrays
 * indexed by their dense keys ({@link DeviceDimension}) and optionally to a small build table
 * ({@link DeviceProbe}), aggregated by the grouped aggregate of the scan. The dimensions and the
 * build table are inputs of a run, not of the scan: one scan of a fact table serves every star
 * query on it, its device buffers allocated once a shape.
 */
public final class StarAggregation {
  private StarAggregation() {}

  /**
   * Build column {@code column} of the probe, summed over the build rows each fact row joins (nulls
   * skipped and counted apart) -- or, with {@link #maskedBy}, only where fact value {@code value}
   * is not null, as SQL's {@code a - COALESCE(b, 0)} is null when {@code a} is.
   */
  public record ProbeSum(int column, int maskedBy) {
    public static ProbeSum of(int column) {
      return new ProbeSum(column, -1);
    }

    public ProbeSum maskedBy(int value) {
      return new ProbeSum(column, value);
    }
  }

  /**
   * The fact rows that join every dimension (inner joins on dense integer keys), grouped by {@code
   * keys} -- dimension attributes, as their codes, and fact columns -- with the sums and counts of
   * {@link DeviceMergeOnReadScan#aggregate(List, List)}.
   */
  public static GroupedAggregate run(
      DeviceMergeOnReadScan scan,
      List<DeviceDimension> dimensions,
      List<DeviceDimension.Key> keys,
      List<DeviceValue> values) {
    return run(scan, dimensions, keys, values, null, ImmutableList.of());
  }

  /**
   * {@link #run(DeviceMergeOnReadScan, List, List, List)}, the fact rows also joined to a probe's
   * build table (or null): each row's sums and counts taken once a build row it joins, and the
   * probe's sums after the values in the results.
   */
  public static GroupedAggregate run(
      DeviceMergeOnReadScan scan,
      List<DeviceDimension> dimensions,
      List<DeviceDimension.Key> keys,
      List<DeviceValue> values,
      DeviceProbe probe,
      List<ProbeSum> probeSums) {
    if (dimensions.isEmpty() && probe == null) {
      throw new IllegalArgumentException("A star aggregate joins a dimension or a probe");
    }
    for (DeviceDimension dimension : dimensions) {
      requireInteger(scan, dimension.factColumn(), "join key");
    }
    if (probe != null) {
      for (DeviceProbe.Part part : probe.parts()) {
        requireInteger(
            scan,
            part.isAttribute() ? dimensions.get(part.dimension()).factColumn() : part.column(),
            "probe key");
      }
    }
    StarStage stage = new StarStage(dimensions, keys, probe, probeSums, values.size());
    return scan.aggregate(stage.groupColumns(), values, stage);
  }

  private static void requireInteger(DeviceMergeOnReadScan scan, String column, String role) {
    Types.NestedField field = scan.schema().findField(column);
    if (field == null) {
      throw new IllegalArgumentException("No column " + column);
    }
    switch (field.type().typeId()) {
      case INTEGER, LONG -> {}
      default -> throw new UnsupportedOperationException("Not an integer " + role + ": " + field);
    }
  }
}
