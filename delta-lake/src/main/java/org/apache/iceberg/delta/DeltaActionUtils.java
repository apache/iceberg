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

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;

public final class DeltaActionUtils {
  private static final String ADD = "add";
  private static final String REMOVE = "remove";

  private DeltaActionUtils() {}

  public static boolean isAdd(Row row) {
    return !row.isNullAt(indexOf(row, ADD));
  }

  public static boolean isRemove(Row row) {
    return !row.isNullAt(indexOf(row, REMOVE));
  }

  public static AddFile getAdd(Row row) {
    return new AddFile(row.getStruct(indexOf(row, ADD)));
  }

  public static RemoveFile getRemove(Row row) {
    return new RemoveFile(row.getStruct(indexOf(row, REMOVE)));
  }

  private static int indexOf(Row row, String name) {
    int idx = row.getSchema().indexOf(name);
    if (idx < 0) {
      throw new IllegalArgumentException("Missing column in Delta action row: " + name);
    }
    return idx;
  }
}
