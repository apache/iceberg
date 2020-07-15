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

package org.apache.iceberg;

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

public class MetadataColumns {

  private MetadataColumns() {
  }

  public static final NestedField FILE_PATH = NestedField.required(
      Integer.MAX_VALUE - 1, "_file", Types.StringType.get(), "Path of the file in which a row is stored");
  public static final NestedField ROW_POSITION = NestedField.required(
      Integer.MAX_VALUE - 2, "_pos", Types.LongType.get(), "Ordinal position of a row in the source data file");

  private static final Map<String, NestedField> META_COLUMNS = ImmutableMap.of(
      FILE_PATH.name(), FILE_PATH,
      ROW_POSITION.name(), ROW_POSITION);

  private static final Set<Integer> META_IDS = META_COLUMNS.values().stream().map(NestedField::fieldId)
      .collect(ImmutableSet.toImmutableSet());

  public static Set<Integer> metadataFieldIds() {
    return META_IDS;
  }
}
