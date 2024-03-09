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
package org.apache.iceberg.expressions;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.StructLike;

public class CountStar<T> extends CountAggregate<T> {
  protected CountStar(BoundTerm<T> term) {
    super(Operation.COUNT_STAR, term);
  }

  @Override
  protected Long countFor(StructLike row) {
    return 1L;
  }

  @Override
  protected boolean hasValue(DataFile file) {
    return file.recordCount() >= 0;
  }

  @Override
  protected Long countFor(DataFile file) {
    long count = file.recordCount();
    if (count < 0) {
      return null;
    }

    return count;
  }
}
