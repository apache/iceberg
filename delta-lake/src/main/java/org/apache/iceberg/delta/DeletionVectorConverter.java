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
package org.apache.iceberg.delta;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.internal.util.Tuple2;

class DeletionVectorConverter {
  private final Engine engine;
  private final String tablePath;

  DeletionVectorConverter(Engine engine, String tablePath) {
    this.engine = engine;
    this.tablePath = tablePath;
  }

  public long[] readDeltaDVPositions(DeletionVectorDescriptor descriptor) {
    Tuple2<DeletionVectorDescriptor, RoaringBitmapArray> tuple =
        DeletionVectorUtils.loadNewDvAndBitmap(engine, tablePath, descriptor);

    return tuple._2.toArray();
  }
}
