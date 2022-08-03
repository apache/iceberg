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

import java.util.List;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

/**
 * Lazily evaluates the schema to see if any field ids are set. Returns true when a first field is
 * found which has field id set
 */
class HasIds extends AvroCustomOrderSchemaVisitor<Boolean, Boolean> {
  @Override
  public Boolean record(Schema record, List<String> names, Iterable<Boolean> fields) {
    return Iterables.any(fields, Boolean.TRUE::equals);
  }

  @Override
  public Boolean field(Schema.Field field, Supplier<Boolean> fieldResult) {
    // see if field id is present, if not, try to find it in the sub tree
    return AvroSchemaUtil.hasFieldId(field) || fieldResult.get();
  }

  @Override
  public Boolean map(Schema map, Supplier<Boolean> value) {
    return AvroSchemaUtil.hasProperty(map, AvroSchemaUtil.KEY_ID_PROP)
        || AvroSchemaUtil.hasProperty(map, AvroSchemaUtil.VALUE_ID_PROP)
        || value.get();
  }

  @Override
  public Boolean array(Schema array, Supplier<Boolean> element) {
    return AvroSchemaUtil.hasProperty(array, AvroSchemaUtil.ELEMENT_ID_PROP) || element.get();
  }

  @Override
  public Boolean union(Schema union, Iterable<Boolean> options) {
    return Iterables.any(options, Boolean.TRUE::equals);
  }

  @Override
  public Boolean primitive(Schema primitive) {
    return false;
  }
}
