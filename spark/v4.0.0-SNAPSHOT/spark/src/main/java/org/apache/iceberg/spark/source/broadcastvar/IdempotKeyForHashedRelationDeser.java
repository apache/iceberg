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
package org.apache.iceberg.spark.source.broadcastvar;

import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;

class IdempotKeyForHashedRelationDeser {
  final BroadcastedJoinKeysWrapper bcjk;

  public IdempotKeyForHashedRelationDeser(BroadcastedJoinKeysWrapper bcjk) {
    this.bcjk = bcjk;
  }

  @Override
  public boolean equals(Object other) {
    if (other != null) {
      return this == other
          || (other instanceof IdempotKeyForHashedRelationDeser
              && this.bcjk.getBroadcastVarId()
                  == ((IdempotKeyForHashedRelationDeser) other).bcjk.getBroadcastVarId());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.bcjk.getBroadcastVarId());
  }
}
