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
package org.apache.iceberg.avro;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;

public class AvroWithTypeByStructureVisitor<T> extends AvroWithPartnerByStructureVisitor<Type, T> {
  @Override
  protected boolean isMapType(Type type) {
    return type.isMapType();
  }

  @Override
  protected boolean isStringType(Type type) {
    return type.isPrimitiveType() && type.asPrimitiveType().typeId() == Type.TypeID.STRING;
  }

  @Override
  protected Type arrayElementType(Type arrayType) {
    return arrayType.asListType().elementType();
  }

  @Override
  protected Type mapKeyType(Type mapType) {
    return mapType.asMapType().keyType();
  }

  @Override
  protected Type mapValueType(Type mapType) {
    return mapType.asMapType().valueType();
  }

  @Override
  protected Pair<String, Type> fieldNameAndType(Type structType, int pos) {
    Types.NestedField field = structType.asStructType().fields().get(pos);
    return Pair.of(field.name(), field.type());
  }

  @Override
  protected Type nullType() {
    return null;
  }
}
