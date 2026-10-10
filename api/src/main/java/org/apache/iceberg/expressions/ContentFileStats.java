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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

final class ContentFileStats {
  private ContentFileStats() {}

  /**
   * Returns stats narrowed to the columns relevant for pruning.
   *
   * <p>An equality delete file may carry stats for extra columns that are not part of its match
   * condition; only its equality fields' stats are relevant for pruning.
   */
  static <V> Map<Integer, V> forColumns(
      ContentFile<?> file, Function<ContentFile<?>, Map<Integer, V>> stats) {
    Map<Integer, V> columnStats = stats.apply(file);
    if (columnStats == null || file.content() != FileContent.EQUALITY_DELETES) {
      return columnStats;
    }

    List<Integer> equalityFieldIds = file.equalityFieldIds();
    if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
      return columnStats;
    }

    return Maps.filterKeys(columnStats, equalityFieldIds::contains);
  }
}
