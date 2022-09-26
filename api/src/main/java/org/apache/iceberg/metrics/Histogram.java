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

public interface Histogram {
  /** Update the histogram with a new value observed. */
  void update(long value);

  /** Return the number of observations. */
  int count();

  /** Calculate the statistics of the observed values. */
  Statistics statistics();

  interface Statistics {
    /**
     * Return the number of values that the statistics computation is based on.
     *
     * <p>If the number of sampling is less than the reservoir size, the sampling count should be
     * returned. Otherwise, the reservoir size is returned.
     */
    int size();

    /** Returns the mean value of the histogram observations. */
    double mean();

    /** Returns the standard deviation of the histogram distribution. */
    double stdDev();

    /** Returns the maximum value of the histogram observations. */
    long max();

    /** Returns the minimum value of the histogram observations. */
    long min();

    /**
     * Returns the percentile value based on the histogram statistics.
     *
     * @param percentile percentile point in double. E.g., 0.75 means 75 percentile. It is up to the
     *     implementation to decide what valid percentile points are supported.
     * @return Value for the given percentile
     */
    long percentile(double percentile);
  }
}
