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

public class CharSequenceUtil {

  private CharSequenceUtil() {}

  public static boolean unequalPaths(CharSequence s1, CharSequence s2) {
    if (s1 == s2) {
      return false;
    }

    int s1Length = s1.length();
    int s2Length = s2.length();

    if (s1Length != s2Length) {
      return true;
    }

    for (int index = s1Length - 1; index >= 0; index--) {
      if (s1.charAt(index) != s2.charAt(index)) {
        return true;
      }
    }

    return false;
  }
}
