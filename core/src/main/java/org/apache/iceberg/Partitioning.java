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

import java.util.List;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.transforms.PartitionSpecVisitor;

public class Partitioning {
  private Partitioning() {
  }

  /**
   * Check whether the spec contains a bucketed partition field.
   *
   * @param spec a partition spec
   * @return true if the spec has field with a bucket transform
   */
  public static boolean hasBucketField(PartitionSpec spec) {
    List<Boolean> bucketList = PartitionSpecVisitor.visit(spec, new PartitionSpecVisitor<Boolean>() {
      @Override
      public Boolean identity(int fieldId, String sourceName, int sourceId) {
        return false;
      }

      @Override
      public Boolean bucket(int fieldId, String sourceName, int sourceId, int width) {
        return true;
      }

      @Override
      public Boolean truncate(int fieldId, String sourceName, int sourceId, int width) {
        return false;
      }

      @Override
      public Boolean year(int fieldId, String sourceName, int sourceId) {
        return false;
      }

      @Override
      public Boolean month(int fieldId, String sourceName, int sourceId) {
        return false;
      }

      @Override
      public Boolean day(int fieldId, String sourceName, int sourceId) {
        return false;
      }

      @Override
      public Boolean hour(int fieldId, String sourceName, int sourceId) {
        return false;
      }

      @Override
      public Boolean alwaysNull(int fieldId, String sourceName, int sourceId) {
        return false;
      }

      @Override
      public Boolean unknown(int fieldId, String sourceName, int sourceId, String transform) {
        return false;
      }
    });

    return bucketList.stream().anyMatch(Boolean::booleanValue);
  }

  /**
   * Create a sort order that will group data for a partition spec.
   * <p>
   * If the partition spec contains bucket columns, the sort order will also have a field to sort by a column that is
   * bucketed in the spec. The column is selected by the highest number of buckets in the transform.
   *
   * @param spec a partition spec
   * @return a sort order that will cluster data for the spec
   */
  public static SortOrder sortOrderFor(PartitionSpec spec) {
    if (spec.isUnpartitioned()) {
      return SortOrder.unsorted();
    }

    SortOrder.Builder builder = SortOrder.builderFor(spec.schema());
    SpecToOrderVisitor converter = new SpecToOrderVisitor(builder);
    PartitionSpecVisitor.visit(spec, converter);

    // columns used for bucketing are high cardinality; add one to the sort at the end
    String bucketColumn = converter.bucketColumn();
    if (bucketColumn != null) {
      builder.asc(bucketColumn);
    }

    return builder.build();
  }

  private static class SpecToOrderVisitor implements PartitionSpecVisitor<Void> {
    private final SortOrder.Builder builder;
    private String bucketColumn = null;
    private int highestNumBuckets = 0;

    private SpecToOrderVisitor(SortOrder.Builder builder) {
      this.builder = builder;
    }

    String bucketColumn() {
      return bucketColumn;
    }

    @Override
    public Void identity(int fieldId, String sourceName, int sourceId) {
      builder.asc(sourceName);
      return null;
    }

    @Override
    public Void bucket(int fieldId, String sourceName, int sourceId, int numBuckets) {
      // the column with highest cardinality is usually the one with the highest number of buckets
      if (numBuckets > highestNumBuckets) {
        this.highestNumBuckets = numBuckets;
        this.bucketColumn = sourceName;
      }
      builder.asc(Expressions.bucket(sourceName, numBuckets));
      return null;
    }

    @Override
    public Void truncate(int fieldId, String sourceName, int sourceId, int width) {
      builder.asc(Expressions.truncate(sourceName, width));
      return null;
    }

    @Override
    public Void year(int fieldId, String sourceName, int sourceId) {
      builder.asc(Expressions.year(sourceName));
      return null;
    }

    @Override
    public Void month(int fieldId, String sourceName, int sourceId) {
      builder.asc(Expressions.month(sourceName));
      return null;
    }

    @Override
    public Void day(int fieldId, String sourceName, int sourceId) {
      builder.asc(Expressions.day(sourceName));
      return null;
    }

    @Override
    public Void hour(int fieldId, String sourceName, int sourceId) {
      builder.asc(Expressions.hour(sourceName));
      return null;
    }

    @Override
    public Void alwaysNull(int fieldId, String sourceName, int sourceId) {
      // do nothing for alwaysNull, it doesn't need to be added to the sort
      return null;
    }
  }
}
