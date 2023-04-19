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
package org.apache.iceberg.orc;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.orc.TypeDescription;

class RemoveIds extends OrcSchemaVisitor<TypeDescription> {

  @Override
  public TypeDescription record(
      TypeDescription record, List<String> names, List<TypeDescription> fields) {
    Preconditions.checkArgument(names.size() == fields.size(), "All fields must have names.");
    TypeDescription struct = TypeDescription.createStruct();

    for (int i = 0; i < fields.size(); i++) {
      struct.addField(names.get(i), fields.get(i));
    }
    return struct;
  }

  @Override
  public TypeDescription list(TypeDescription array, TypeDescription element) {
    return TypeDescription.createList(element);
  }

  @Override
  public TypeDescription map(TypeDescription map, TypeDescription key, TypeDescription value) {
    return TypeDescription.createMap(key, value);
  }

  @Override
  public TypeDescription primitive(TypeDescription primitive) {
    return removeIcebergAttributes(primitive.clone());
  }

  private static TypeDescription removeIcebergAttributes(TypeDescription orcType) {
    orcType.removeAttribute(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE);
    return orcType;
  }
}
