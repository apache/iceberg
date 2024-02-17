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
package org.apache.iceberg.spark.source.broadcastvar.broadcastutils;

import java.util.AbstractList;
import java.util.List;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Literals;
import org.apache.spark.sql.catalyst.bcvar.ArrayWrapper;

public class LiteralListWrapper<T> extends AbstractList<Literal<T>> {

  private final ArrayWrapper<T> underlyingArray;

  public LiteralListWrapper( ArrayWrapper<T> underlyingArray) {
    this.underlyingArray = underlyingArray;
  }

  @Override
  public Literal<T> get(int index) {
    return Literals.from(this.underlyingArray.get(index));
  }

  @Override
  public int size() {
    return this.underlyingArray.getLength();
  }

  public ArrayWrapper<T> getUnderlyingArray() {
    return this.underlyingArray;
  }
}
