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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.util;

import java.util.Arrays;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestCharSequenceSet {

  // This test just verifies https://errorprone.info/bugpattern/CollectionUndefinedEquality
  @Test
  public void testSearchingInCharSequenceCollection() {
    Set<CharSequence> set = CharSequenceSet.of(Arrays.asList("abc", new StringBuffer("def")));
    Assertions.assertThat(set).contains("abc");
    Assertions.assertThat(set.stream().anyMatch("def"::contains)).isTrue();

    // this would fail with a normal Set<CharSequence>
    Assertions.assertThat(set.contains("def")).isTrue();
  }
}
