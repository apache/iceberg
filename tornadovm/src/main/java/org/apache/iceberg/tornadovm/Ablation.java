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
package org.apache.iceberg.tornadovm;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Switches off, for an experiment, the decisions the scan takes from Iceberg's metadata, to measure
 * what each one buys: {@code -Diceberg.tornadovm.ablate=batching,nullcounts,...}.
 *
 * <ul>
 *   <li>{@code batching} -- files batch together only by size, not by shared equality-delete sets:
 *       every file is its own batch (one cuDF read, one key-set probe setup each);
 *   <li>{@code nullcounts} -- null counts unknown: every optional column's validity is copied;
 *   <li>{@code bounds} -- column bounds unknown: no device sum can be proven exact;
 *   <li>{@code density} -- dense keys not checked from statistics at planning time, only when the
 *       dimension has been built (a non-dense key then fails after the work, not before it);
 *   <li>{@code dense} -- footer bounds not used to group by a key of few values without a hash
 *       table: every group-by goes to cuDF's hash group-by.
 * </ul>
 */
public final class Ablation {
  private static final Set<String> OFF =
      Arrays.stream(System.getProperty("iceberg.tornadovm.ablate", "").split(","))
          .map(String::trim)
          .filter(s -> !s.isEmpty())
          .collect(Collectors.toUnmodifiableSet());

  private Ablation() {}

  /** Whether the decision is switched off for this JVM. */
  public static boolean off(String decision) {
    return OFF.contains(decision);
  }
}
