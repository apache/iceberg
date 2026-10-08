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

/**
 * Combines the vertical splits of a row into a single row.
 *
 * @param <D> the type of the data records that are combined
 */
public interface Stitcher<D> {
  /**
   * Combines one unit of each vertical split into a unit of the projection.
   *
   * <p>The list is reused by the caller, so implementations must not retain it.
   */
  D stitch(List<D> parts, int count);

  /** Returns a range of rows of a unit; only supported by vectorized models. */
  default D slice(D unit, int offset, int count) {
    throw new UnsupportedOperationException("Slicing is not supported for row based models");
  }
}
