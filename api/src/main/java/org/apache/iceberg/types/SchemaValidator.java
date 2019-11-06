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

package org.apache.iceberg.types;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;

public class SchemaValidator extends TypeUtil.SchemaVisitor<List<String>>  {
  private static final Joiner DOT = Joiner.on(".");

  private final Set<String> fieldSet = new HashSet<>();
  private final List<String> invalidFields = new ArrayList<>();

  @Override
  public List<String> schema(Schema schema, List<String> structResult) {
    return invalidFields;
  }

  @Override
  public List<String> struct(Types.StructType struct, List<List<String>> fieldResults) {
    return invalidFields;
  }


  @Override
  public List<String> field(Types.NestedField field, List<String> fieldResult) {
    addField(field.name());
    return null;
  }

  @Override
  public List<String> list(Types.ListType list, List<String> elementResult) {
    // ignore list since we don't push name for the ListType during visiting
    return null;
  }

  @Override
  public List<String> map(Types.MapType map, List<String> keyResult, List<String> valueResult) {
    // ignore map since we don't push name for the MapType during visiting
    return null;
  }

  private void addField(String name) {
    String fullName = name;
    if (!fieldNames().isEmpty()) {
      fullName = DOT.join(DOT.join(fieldNames().descendingIterator()), name);
    }
    if (fieldSet.contains(fullName)) {
      invalidFields.add(fullName);
    }
    fieldSet.add(fullName);
  }
}
