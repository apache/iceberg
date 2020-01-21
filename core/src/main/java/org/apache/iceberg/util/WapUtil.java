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

package org.apache.iceberg.util;

import com.google.common.base.Preconditions;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;

public class WapUtil {

  private WapUtil() {
  }

  /**
   * Check if a given wap id was published in a given table state
   * @param table a {@link Table}
   * @param wapId the wap id associated with snapshot that needs to be checked
   * @return true if the wap id was published to the table's state, false if not
   */
  public static boolean isWapIdPublished(Table table, String wapId) {
    Preconditions.checkArgument(table instanceof HasTableOperations,
        "Cannot check if WAP Id was published on table that doesn't expose its TableOperations");
    return ((HasTableOperations) table).operations()
        .current().snapshotsByWapId().containsKey(wapId);
  }

}
