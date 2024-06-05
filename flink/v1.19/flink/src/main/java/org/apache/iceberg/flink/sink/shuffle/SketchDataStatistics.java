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
package org.apache.iceberg.flink.sink.shuffle;

import java.util.Arrays;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;

/** MapDataStatistics uses map to count key frequency */
class SketchDataStatistics implements DataStatistics {

  private final ReservoirItemsSketch<SortKey> sketch;

  SketchDataStatistics(int reservoirSize) {
    this.sketch = ReservoirItemsSketch.newInstance(reservoirSize);
  }

  SketchDataStatistics(ReservoirItemsSketch<SortKey> sketchStats) {
    this.sketch = sketchStats;
  }

  @Override
  public StatisticsType type() {
    return StatisticsType.Sketch;
  }

  @Override
  public boolean isEmpty() {
    return sketch.getNumSamples() == 0;
  }

  @Override
  public void add(SortKey sortKey) {
    // clone the sort key first because input sortKey object can be reused
    SortKey copiedKey = sortKey.copy();
    sketch.update(copiedKey);
  }

  @Override
  public Object result() {
    return sketch;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("sketch", sketch).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof SketchDataStatistics)) {
      return false;
    }

    ReservoirItemsSketch<SortKey> otherSketch = ((SketchDataStatistics) o).sketch;
    return Objects.equal(sketch.getK(), otherSketch.getK())
        && Objects.equal(sketch.getN(), otherSketch.getN())
        && Arrays.deepEquals(sketch.getSamples(), otherSketch.getSamples());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sketch.getK(), sketch.getN(), sketch.getSamples());
  }
}
