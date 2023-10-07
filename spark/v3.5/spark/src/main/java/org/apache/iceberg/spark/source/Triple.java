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
package org.apache.iceberg.spark.source;

import org.apache.iceberg.relocated.com.google.common.base.Objects;

public class Triple<A, B, C> {
  private final A element1;
  private final B element2;
  private final C element3;

  public Triple(A element1, B element2, C element3) {
    this.element1 = element1;
    this.element2 = element2;
    this.element3 = element3;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;

    return element1.equals(triple.element1)
        && element2.equals(triple.element2)
        && element3.equals(triple.element3);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(element1, element2, element3);
  }

  public A getElement1() {
    return this.element1;
  }

  public B getElement2() {
    return this.element2;
  }

  public C getElement3() {
    return this.element3;
  }
}
