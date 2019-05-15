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

package org.apache.iceberg.catalog;

import java.util.Arrays;

import static java.util.stream.Collectors.joining;

/**
 * Identifies a namespace in iceberg catalog
 */
public class Namespace {
  private final String[] levels;
  private static final Namespace EMPTY = new Namespace(new String[] {});

  private Namespace(String[] levels) {
    this.levels = levels;
  }

  public String[] levels() {
    return levels;
  }

  public boolean isEmpty() {
    return this.equals(Namespace.EMPTY);
  }

  public static Namespace namespace(String[] levels) {
    if (levels == null || levels.length == 0) {
      return Namespace.EMPTY;
    }

    return new Namespace(levels);
  }

  public static Namespace empty() {
    return EMPTY;
  }

  @Override
  public String toString() {
    // assumes a level it self won't have a period in it, otherwise it will be ambiguous
    return Arrays.stream(levels).collect(joining("."));
  }
}
