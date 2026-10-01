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
package org.apache.iceberg.flink.sink.shuffle;

import static org.apache.iceberg.flink.sink.shuffle.DataDistributionUtil.binarySearchIndex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TestDataDistributionUtil {
  @Test
  public void testBinarySearchIndex() {
    long[] weightsUDF = {10, 20, 30, 40, 50};
    assertThat(binarySearchIndex(weightsUDF, 0)).isEqualTo(0);
    assertThat(binarySearchIndex(weightsUDF, 9)).isEqualTo(0);
    assertThat(binarySearchIndex(weightsUDF, 10)).isEqualTo(1);
    assertThat(binarySearchIndex(weightsUDF, 15)).isEqualTo(1);
    assertThat(binarySearchIndex(weightsUDF, 20)).isEqualTo(2);
    assertThat(binarySearchIndex(weightsUDF, 29)).isEqualTo(2);
    assertThat(binarySearchIndex(weightsUDF, 30)).isEqualTo(3);
    assertThat(binarySearchIndex(weightsUDF, 31)).isEqualTo(3);
    assertThat(binarySearchIndex(weightsUDF, 40)).isEqualTo(4);

    // Test with a target that is out of range
    assertThatThrownBy(() -> binarySearchIndex(weightsUDF, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("target weight must be non-negative");
    assertThatThrownBy(() -> binarySearchIndex(weightsUDF, 50))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("target weight is out of range");
  }
}
