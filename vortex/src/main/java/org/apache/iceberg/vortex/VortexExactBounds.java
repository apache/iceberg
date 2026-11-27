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
package org.apache.iceberg.vortex;

import java.util.Map;
import org.apache.iceberg.util.Pair;

/**
 * A value writer that tracks exact bounds for columns whose Vortex statistics are too coarse to use
 * as written.
 *
 * <p>Vortex reports a string column's bounds as a truncated prefix range, which is fine for pruning
 * but loses any information a caller needs exactly. Iceberg recognizes a position delete file as
 * covering a single data file only when {@code file_path}'s lower and upper bounds are equal, so a
 * writer that needs that has to track the column as it writes and report it here.
 */
public interface VortexExactBounds {
  /**
   * Exact lower and upper bounds, keyed by Iceberg field id, for the columns this writer tracked.
   * Columns absent from the map keep the bounds Vortex reported.
   */
  Map<Integer, Pair<Object, Object>> exactBounds();
}
