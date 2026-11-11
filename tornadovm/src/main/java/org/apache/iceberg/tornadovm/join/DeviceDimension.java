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

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * A dimension table of a star join, as arrays indexed by its key: the join of the fact table's
 * {@code factColumn} with a dimension whose keys are dense integers ({@code firstKey}, {@code
 * firstKey + 1}, ...) is a lookup, not a hash join. {@code keep[key - firstKey]} says whether the
 * dimension has that key and its row passes the query's filters on the dimension; an inner join
 * drops every fact row whose key is null, out of range or not kept. Each attribute is an array of
 * codes, an int from 0 a key, that a query can group by in place of the key (a dictionary code of
 * the dimension's {@code i_brand}, say); the caller decodes them.
 *
 * <p>The caller builds the arrays from the (small) dimension table, with any filter it can
 * evaluate, so the device sees only ints.
 */
public final class DeviceDimension {
  private final String factColumn;
  private final int firstKey;
  private final int[] keep;
  private final List<int[]> attributes;

  private DeviceDimension(String factColumn, int firstKey, int[] keep, List<int[]> attributes) {
    for (int[] codes : attributes) {
      if (codes.length != keep.length) {
        throw new IllegalArgumentException(
            "An attribute holds " + codes.length + " codes for " + keep.length + " keys");
      }
    }
    this.factColumn = factColumn;
    this.firstKey = firstKey;
    this.keep = keep;
    this.attributes = attributes;
  }

  /** {@code keep}: per key from {@code firstKey}, whether a fact row with that key joins. */
  public static DeviceDimension of(String factColumn, int firstKey, boolean[] keep) {
    int[] kept = new int[keep.length];
    for (int k = 0; k < keep.length; k++) {
      kept[k] = keep[k] ? 1 : 0;
    }
    return new DeviceDimension(factColumn, firstKey, kept, ImmutableList.of());
  }

  /**
   * The same join, with one more attribute to group by: a code per key, from 0 (codes are indices
   * into the caller's values, so the device can also aggregate them densely).
   */
  public DeviceDimension withAttribute(int[] codes) {
    for (int code : codes) {
      if (code < 0) {
        throw new IllegalArgumentException("A negative attribute code: " + code);
      }
    }
    return new DeviceDimension(
        factColumn,
        firstKey,
        keep,
        ImmutableList.<int[]>builder().addAll(attributes).add(codes.clone()).build());
  }

  String factColumn() {
    return factColumn;
  }

  int firstKey() {
    return firstKey;
  }

  int keys() {
    return keep.length;
  }

  int[] keep() {
    return keep;
  }

  List<int[]> attributes() {
    return attributes;
  }

  /** How many attributes this dimension carries. */
  public int attributeCount() {
    return attributes.size();
  }

  /** Ints this dimension takes on the device: the mask and every attribute. */
  long ints() {
    return (long) keys() * (1 + attributes.size());
  }

  /**
   * A group key of a star aggregate: a fact column, or an attribute of one of the query's
   * dimensions.
   */
  public record Key(String column, int dimension, int attribute) {
    /** The fact table's own column. */
    public static Key column(String name) {
      return new Key(name, -1, -1);
    }

    /** Attribute {@code attribute} of dimension {@code dimension}, as its codes. */
    public static Key attribute(int dimension, int attribute) {
      return new Key(null, dimension, attribute);
    }

    boolean isAttribute() {
      return dimension >= 0;
    }
  }

  @Override
  public String toString() {
    return factColumn
        + " -> ["
        + firstKey
        + ", "
        + (firstKey + keys())
        + "), "
        + Arrays.stream(keep).sum()
        + " kept, "
        + attributes.size()
        + " attributes";
  }
}
