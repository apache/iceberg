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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class TestArrayUtil {

  @Test
  public void testStrictlyAscendingLongArrays() {
    long[] emptyArray = new long[0];
    assertThat(ArrayUtil.isStrictlyAscending(emptyArray)).isTrue();

    long[] singleElementArray = new long[] {1};
    assertThat(ArrayUtil.isStrictlyAscending(singleElementArray)).isTrue();

    long[] strictlyAscendingArray = new long[] {1, 2, 3};
    assertThat(ArrayUtil.isStrictlyAscending(strictlyAscendingArray)).isTrue();

    long[] descendingArray = new long[] {3, 2, 1};
    assertThat(ArrayUtil.isStrictlyAscending(descendingArray)).isFalse();

    long[] ascendingArray = new long[] {1, 2, 2, 3};
    assertThat(ArrayUtil.isStrictlyAscending(ascendingArray)).isFalse();
  }
}
