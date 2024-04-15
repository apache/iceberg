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

import java.util.Arrays;
import java.util.function.Function;
import org.apache.spark.sql.catalyst.bcvar.ArrayWrapper;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;

public final class BroadcastUtil {

  private BroadcastUtil() {
    throw new UnsupportedOperationException("Utility class instance cannot be constructed");
  }

  // TODO: Asif  verify that the data going into transform is of correct type
  // and whether date fixing needs to be done or not
  public static <S, T> ArrayWrapper<T> evaluateLiteralWithTransform(
      BroadcastedJoinKeysWrapper bcVar, Function<S, T> transform, boolean fixDate) {
    boolean is1D = bcVar.getTotalJoinKeys() == 1;
    int index = bcVar.getKeyIndex();

    // There should not be any need to fix date as the data is coming from BHJ
    Object[] arr =
        Arrays.stream((Object[]) bcVar.getKeysArray().getBaseArray())
            .map(
                ele -> {
                  S value;
                  if (is1D) {
                    value = (S) ele;
                  } else {
                    value = (S) (((Object[]) ele)[index]);
                  }
                  return transform.apply(value);
                })
            .toArray();
    return (ArrayWrapper<T>) ArrayWrapper.wrapArray(arr, is1D, bcVar.getKeyIndex());
  }
}
