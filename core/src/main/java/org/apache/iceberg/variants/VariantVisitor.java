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

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class VariantVisitor<R> {
  public R object(VariantObject object, List<String> fieldNames, List<R> fieldResults) {
    return null;
  }

  public R array(VariantArray array, List<R> elementResults) {
    return null;
  }

  public R primitive(VariantPrimitive<?> primitive) {
    return null;
  }

  public void beforeArrayElement(int index) {}

  public void afterArrayElement(int index) {}

  public void beforeObjectField(String fieldName) {}

  public void afterObjectField(String fieldName) {}

  public static <R> R visit(Variant variant, VariantVisitor<R> visitor) {
    return visit(variant.value(), visitor);
  }

  public static <R> R visit(VariantValue value, VariantVisitor<R> visitor) {
    switch (value.type()) {
      case ARRAY:
        VariantArray array = value.asArray();
        List<R> elementResults = Lists.newArrayList();
        for (int index = 0; index < array.numElements(); index += 1) {
          visitor.beforeArrayElement(index);
          try {
            elementResults.add(visit(array.get(index), visitor));
          } finally {
            visitor.afterArrayElement(index);
          }
        }

        return visitor.array(array, elementResults);

      case OBJECT:
        VariantObject object = value.asObject();
        List<String> fieldNames = Lists.newArrayList();
        List<R> fieldResults = Lists.newArrayList();
        for (String fieldName : object.fieldNames()) {
          fieldNames.add(fieldName);
          visitor.beforeObjectField(fieldName);
          try {
            fieldResults.add(visit(object.get(fieldName), visitor));
          } finally {
            visitor.afterObjectField(fieldName);
          }
        }

        return visitor.object(object, fieldNames, fieldResults);

      default:
        return visitor.primitive(value.asPrimitive());
    }
  }
}
