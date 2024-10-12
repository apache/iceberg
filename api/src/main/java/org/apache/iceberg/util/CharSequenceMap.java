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

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.JavaHashes;

/** A map that uses char sequences as keys and compares them by value. */
public abstract class CharSequenceMap {
    private static final Comparator<CharSequence> COMPARATOR = Comparators.charSequences();

    public static <V> Map<CharSequence, V> create() {
        return new TreeMap<>(COMPARATOR) {
            // TreeMap.equals is value-based (obeying provided comparator), but TreeMap.hashCode does not comply (because comparator cannot hash) and needs to be fixed.
            @Override
            public int hashCode() {
                // hash computation follows AbstractMap.hashCode with the modification for hashing keys to be char sequence value-based
                int h = 0;
                for (Map.Entry<CharSequence, V> entry : entrySet()) {
                    h += (JavaHashes.hashCode(entry.getKey()) ^ Objects.hashCode(entry.getValue()));
                }
                return h;
            }
        };
    }
}
