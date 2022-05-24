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

package org.apache.iceberg.data;

import java.util.Map;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types.StructType;

public interface Record extends StructLike {
  StructType struct();

  Object getField(String name);

  void setField(String name, Object value);

  Object get(int pos);

  Record copy();

  Record copy(Map<String, Object> overwriteValues);

  default Record copy(String field, Object value) {
    Map<String, Object> overwriteValues = Maps.newHashMapWithExpectedSize(1);
    overwriteValues.put(field, value);
    return copy(overwriteValues);
  }

  default Record copy(String field1, Object value1, String field2, Object value2) {
    Map<String, Object> overwriteValues = Maps.newHashMapWithExpectedSize(2);
    overwriteValues.put(field1, value1);
    overwriteValues.put(field2, value2);
    return copy(overwriteValues);
  }

  default Record copy(String field1, Object value1, String field2, Object value2, String field3, Object value3) {
    Map<String, Object> overwriteValues = Maps.newHashMapWithExpectedSize(3);
    overwriteValues.put(field1, value1);
    overwriteValues.put(field2, value2);
    overwriteValues.put(field3, value3);
    return copy(overwriteValues);
  }

  default Record copy(String field1, Object value1, String field2, Object value2, String field3, Object value3,
                      String field4, Object value4, String field5, Object value5, String field6, Object value6,
                      String field7, Object value7, String field8, Object value8, String field9, Object value9,
                      String field10, Object value10, String field11, Object value11) {
    Map<String, Object> overwriteValues = Maps.newHashMapWithExpectedSize(11);
    overwriteValues.put(field1, value1);
    overwriteValues.put(field2, value2);
    overwriteValues.put(field3, value3);
    overwriteValues.put(field4, value4);
    overwriteValues.put(field5, value5);
    overwriteValues.put(field6, value6);
    overwriteValues.put(field7, value7);
    overwriteValues.put(field8, value8);
    overwriteValues.put(field9, value9);
    overwriteValues.put(field10, value10);
    overwriteValues.put(field11, value11);
    return copy(overwriteValues);
  }
}
