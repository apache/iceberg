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
package org.apache.iceberg.formats;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;

/**
 * Locates each top-level field of a projection within the vertical splits a row is stitched from.
 */
public class StitchLayout {
  private final int[] splits;
  private final int[] ordinals;

  private StitchLayout(int[] splits, int[] ordinals) {
    this.splits = splits;
    this.ordinals = ordinals;
  }

  public static StitchLayout of(Schema projection, List<Schema> verticalSplits) {
    Preconditions.checkArgument(projection != null, "Invalid projection: null");
    Preconditions.checkArgument(verticalSplits != null, "Invalid vertical splits: null");

    Map<Integer, Pair<Integer, Integer>> locations = Maps.newHashMap();
    for (int split = 0; split < verticalSplits.size(); split += 1) {
      List<Types.NestedField> columns = verticalSplits.get(split).columns();
      for (int ordinal = 0; ordinal < columns.size(); ordinal += 1) {
        Types.NestedField field = columns.get(ordinal);
        Preconditions.checkArgument(
            locations.putIfAbsent(field.fieldId(), Pair.of(split, ordinal)) == null,
            "Cannot stitch field %s: provided by more than one split",
            field);
      }
    }

    List<Types.NestedField> fields = projection.columns();
    int[] splits = new int[fields.size()];
    int[] ordinals = new int[fields.size()];
    for (int pos = 0; pos < fields.size(); pos += 1) {
      Types.NestedField field = fields.get(pos);
      Pair<Integer, Integer> location = locations.get(field.fieldId());
      Preconditions.checkArgument(
          location != null, "Cannot stitch field %s: not provided by any split", field);
      splits[pos] = location.first();
      ordinals[pos] = location.second();
    }

    return new StitchLayout(splits, ordinals);
  }

  /** Returns the number of fields of the projection. */
  public int size() {
    return ordinals.length;
  }

  /**
   * Returns the index of the vertical split that provides a field of the projection.
   *
   * @param pos the position of the field in the projection
   * @return the index of the vertical split providing the field
   */
  public int split(int pos) {
    return splits[pos];
  }

  /**
   * Returns the position of a field of the projection within the vertical split that provides it.
   *
   * @param pos the position of the field in the projection
   * @return the position of the field in its vertical split
   */
  public int ordinal(int pos) {
    return ordinals[pos];
  }
}
