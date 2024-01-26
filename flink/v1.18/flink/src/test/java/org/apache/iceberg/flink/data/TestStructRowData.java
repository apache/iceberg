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
package org.apache.iceberg.flink.data;

import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.flink.DataGenerator;
import org.apache.iceberg.flink.DataGenerators;
import org.apache.iceberg.flink.TestHelpers;
import org.junit.Test;

public class TestStructRowData {

  protected void testConverter(DataGenerator dataGenerator) {
    StructRowData converter = new StructRowData(dataGenerator.icebergSchema().asStruct());
    GenericRecord expected = dataGenerator.generateIcebergGenericRecord();
    StructRowData actual = converter.setStruct(expected);
    TestHelpers.assertRowData(dataGenerator.icebergSchema(), expected, actual);
  }

  @Test
  public void testPrimitiveTypes() {
    testConverter(new DataGenerators.Primitives());
  }

  @Test
  public void testStructOfPrimitive() {
    testConverter(new DataGenerators.StructOfPrimitive());
  }

  @Test
  public void testStructOfArray() {
    testConverter(new DataGenerators.StructOfArray());
  }

  @Test
  public void testStructOfMap() {
    testConverter(new DataGenerators.StructOfMap());
  }

  @Test
  public void testStructOfStruct() {
    testConverter(new DataGenerators.StructOfStruct());
  }

  @Test
  public void testArrayOfPrimitive() {
    testConverter(new DataGenerators.ArrayOfPrimitive());
  }

  @Test
  public void testArrayOfArray() {
    testConverter(new DataGenerators.ArrayOfArray());
  }

  @Test
  public void testArrayOfMap() {
    testConverter(new DataGenerators.ArrayOfMap());
  }

  @Test
  public void testArrayOfStruct() {
    testConverter(new DataGenerators.ArrayOfStruct());
  }

  @Test
  public void testMapOfPrimitives() {
    testConverter(new DataGenerators.MapOfPrimitives());
  }

  @Test
  public void testMapOfArray() {
    testConverter(new DataGenerators.MapOfArray());
  }

  @Test
  public void testMapOfMap() {
    testConverter(new DataGenerators.MapOfMap());
  }

  @Test
  public void testMapOfStruct() {
    testConverter(new DataGenerators.MapOfStruct());
  }
}
