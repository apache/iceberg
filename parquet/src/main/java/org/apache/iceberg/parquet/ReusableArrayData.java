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

package org.apache.iceberg.parquet;

public interface ReusableArrayData {
  Object[] EMPTY = new Object[0];

  Object[] values();
  void setValues(Object[] array);

  default void grow() {
    if (values().length == 0) {
      this.setValues(new Object[20]);
    } else {
      Object[] old = values();
      this.setValues(new Object[old.length << 1]);
      // copy the old array in case it has values that can be reused
      System.arraycopy(old, 0, values(), 0, old.length);
    }
  }

  default void update(int ordinal, Object value) {
    this.values()[ordinal] = value;
  }

  default int capacity() {
    return values().length;
  }

  void setNumElements(int numElements);

  int getNumElements();

  default Object getObj(int ordinal) {
    return values()[ordinal];
  }
}
