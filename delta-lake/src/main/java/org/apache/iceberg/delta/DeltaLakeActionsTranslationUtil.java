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

/**
 * Util class helps to handle json operations for <a
 * href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Actions">delta action</a>
 */
public class DeltaLakeActionsTranslationUtil {
  private DeltaLakeActionsTranslationUtil() {}

  public static boolean isAdd(Row row) {
    return !row.isNullAt(getOrdinal(row, "add"));
  }

  public static AddFile toAdd(Row row) {
    return new AddFile(row.getStruct(row.getSchema().indexOf("add")));
  }

  public static boolean isRemove(Row row) {
    return !row.isNullAt(getOrdinal(row, "remove"));
  }

  public static RemoveFile toRemove(Row row) {
    return new RemoveFile(row.getStruct(row.getSchema().indexOf("remove")));
  }

  public static boolean isMetaData(Row row) {
    return !row.isNullAt(getOrdinal(row, "metaData"));
  }

  public static boolean isTxn(Row row) {
    return !row.isNullAt(getOrdinal(row, "txn"));
  }

  public static boolean isProtocol(Row row) {
    return !row.isNullAt(getOrdinal(row, "protocol"));
  }

  public static boolean isCdc(Row row) {
    return !row.isNullAt(getOrdinal(row, "cdc"));
  }

  public static boolean isCommitInfo(Row row) {
    return !row.isNullAt(getOrdinal(row, "commitInfo"));
  }

  private static int getOrdinal(Row row, String filedName) {
    return row.getSchema().indexOf(filedName);
  }
}
