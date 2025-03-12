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
package org.apache.iceberg.variants;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class ShreddedArray implements VariantArray {
  private List<VariantValue> elements;

  ShreddedArray() {
    this.elements = Lists.newArrayList();
  }

  @Override
  public VariantValue get(int index) {
    return elements.get(index);
  }

  @Override
  public int numElements() {
    return elements.size();
  }

  public void add(VariantValue value) {
    elements.add(value);
  }

  @Override
  public int sizeInBytes() {
    throw new UnsupportedOperationException("sizeInBytes in ShreddedArray is not supported yet");
  }

  @Override
  public int writeTo(ByteBuffer buffer, int offset) {
    throw new UnsupportedOperationException("writeTo in ShreddedArray is not supported yet");
  }
}
