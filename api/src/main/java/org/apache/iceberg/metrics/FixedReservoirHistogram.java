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
package org.apache.iceberg.metrics;

import java.util.Arrays;
import java.util.Random;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** A {@link Histogram} implementation with reservoir sampling. */
public class FixedReservoirHistogram implements Histogram {
  private final Random rand;
  private final long[] measurements;
  private int count;

  public FixedReservoirHistogram(int reservoirSize) {
    this.rand = new Random();
    this.measurements = new long[reservoirSize];
    this.count = 0;
  }

  @Override
  public synchronized int count() {
    return count;
  }

  @Override
  public synchronized void update(long value) {
    count += 1;
    int index = count <= measurements.length ? count - 1 : rand.nextInt(count);
    if (index < measurements.length) {
      measurements[index] = value;
    }
  }

  /**
   * Naive algorithm for calculating variance:
   * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
   */
  @Override
  public Statistics statistics() {
    int size = (int) Math.min(count, (long) measurements.length);
    if (size == 0) {
      return UniformWeightStatistics.EMPTY;
    }

    long[] values = new long[size];
    synchronized (this) {
      System.arraycopy(measurements, 0, values, 0, size);
    }

    double sum = 0.0d;
    double sumSquares = 0.0d;
    for (long x : values) {
      sum += x;
      // Convert to double value to avoid potential overflow of square
      double value = (double) x;
      sumSquares += value * value;
    }

    double mean = sum / size;
    // Use the uncorrected estimator. Bessel's correction is not a good fit in this context.
    // https://en.wikipedia.org/wiki/Bessel%27s_correction#Caveats
    double variance = (sumSquares - (sum * sum) / size) / size;
    double stdDev = Math.sqrt(variance);
    return new UniformWeightStatistics(values, mean, stdDev);
  }

  private static class UniformWeightStatistics implements Statistics {
    private static final UniformWeightStatistics EMPTY =
        new UniformWeightStatistics(new long[0], 0.0, 0.0);

    private final long[] values;
    private final double mean;
    private final double stdDev;

    private UniformWeightStatistics(long[] values, double mean, double stdDev) {
      this.values = values;
      this.mean = mean;
      this.stdDev = stdDev;

      // since the input values is already a copied array,
      // there is no need to copy again.
      Arrays.sort(this.values);
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    public double mean() {
      return mean;
    }

    @Override
    public double stdDev() {
      return stdDev;
    }

    @Override
    public long max() {
      return values.length == 0 ? 0L : values[values.length - 1];
    }

    @Override
    public long min() {
      return values.length == 0 ? 0L : values[0];
    }

    @Override
    public long percentile(double percentile) {
      Preconditions.checkArgument(
          !Double.isNaN(percentile) && percentile >= 0.0 && percentile <= 1.0,
          "Percentile point cannot be outside the range of [0.0 - 1.0]: %s",
          percentile);
      if (values.length == 0) {
        return 0L;
      } else {
        double position = percentile * values.length;
        int index = (int) position;
        if (index < 1) {
          return values[0];
        } else if (index >= values.length) {
          return values[values.length - 1];
        } else {
          double lower = (double) values[index - 1];
          double upper = (double) values[index];
          double interpolated = lower + (position - Math.floor(position)) * (upper - lower);
          return (long) interpolated;
        }
      }
    }
  }
}
