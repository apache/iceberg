/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.junit.Assert;
import org.junit.Test;

public class TestNameMappingWithAvroSchema {
  @Test
  public void testNameMappingWithAvroSchema() {
    NameMappingWithAvroSchema nameMappingWithAvroSchema = new NameMappingWithAvroSchema();
    // Create example Avro schema
    Schema schema = SchemaBuilder
      .record("test")
      .fields()
      .name("id").type().intType().noDefault()
      .name("data").type().stringType().noDefault()
      .name("location").type().record("location")
      .fields()
      .name("lat").type().doubleType().noDefault()
      .name("long").type().doubleType().noDefault()
      .endRecord().noDefault()
      .name("friends").type().array().items().stringType().noDefault()
      .name("simpleUnion").type().unionOf().nullType().and().stringType().endUnion().noDefault()
      .name("complexUnion").type().unionOf().nullType().and().stringType().and().record("innerRecord1")
      .fields()
      .name("lat").type().doubleType().noDefault()
      .name("long").type().doubleType().noDefault()
      .endRecord()
      .and()
      .record("innerRecord2")
      .fields()
      .name("lat").type().doubleType().noDefault()
      .name("long").type().doubleType().noDefault().endRecord()
      .and()
      .record("innerRecord3")
      .fields()
      .name("innerUnion").type().unionOf().stringType().and().intType().endUnion().noDefault().endRecord().endUnion().noDefault()
      .name("timezone").type().enumeration("timezone").symbols("UTC", "PST", "EST").noDefault()
      .name("bitmap").type().fixed("bitmap").size(16).noDefault()
      .endRecord();

    // Convert Avro schema to Iceberg schema
    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(schema);
   MappedFields expected = MappedFields.of(
     MappedField.of(0, "id"),
     MappedField.of(1, "data"),
     MappedField.of(2, "location", MappedFields.of(
       MappedField.of(8, "lat"),
       MappedField.of(9, "long"))),
     MappedField.of(3, "friends", MappedFields.of(
       MappedField.of(10, "element"))),
     MappedField.of(4, "simpleUnion"),
     MappedField.of(5, "complexUnion", MappedFields.of(
       MappedField.of(19, "\"string\""),
       MappedField.of(20, "innerRecord1", MappedFields.of(
         MappedField.of(11, "lat"),
         MappedField.of(12, "long"))),
       MappedField.of(21, "innerRecord2", MappedFields.of(
         MappedField.of(13, "lat"),
         MappedField.of(14, "long"))),
       MappedField.of(22, "innerRecord3", MappedFields.of(
         MappedField.of(18, "innerUnion",MappedFields.of(
         MappedField.of(15, "\"string\""),
         MappedField.of(16, "\"int\""))))))),
     MappedField.of(6, "timezone"),
     MappedField.of(7, "bitmap"));
    Assert.assertEquals(expected, nameMappingWithAvroSchema.visit(icebergSchema, schema, nameMappingWithAvroSchema));
  }
}
