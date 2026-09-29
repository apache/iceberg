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
import org.apache.iceberg.Schema;

/**
 * Creates {@link Stitcher}s for an object model.
 *
 * @param <D> the type of the data records that are combined
 */
public interface StitcherBuilder<D> {
  /** Returns the object model class the stitchers combine. */
  Class<? extends D> type();

  /**
   * Returns a stitcher producing rows of the projection from the given vertical splits.
   *
   * @throws IllegalArgumentException if a field of the projection is not provided by any split
   */
  Stitcher<D> build(Schema projection, List<Schema> verticalSplits);
}
