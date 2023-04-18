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
package org.apache.iceberg.flink;

import org.junit.Test;

public abstract class AvroGenericRecordConverterBase {
  protected abstract void testConverter(DataGenerator dataGenerator) throws Exception;

  @Test
  public void testPrimitiveTypes() throws Exception {
    testConverter(new DataGenerators.Primitives());
  }

  @Test
  public void testStructOfPrimitive() throws Exception {
    testConverter(new DataGenerators.StructOfPrimitive());
  }

  @Test
  public void testStructOfArray() throws Exception {
    testConverter(new DataGenerators.StructOfArray());
  }

  @Test
  public void testStructOfMap() throws Exception {
    testConverter(new DataGenerators.StructOfMap());
  }

  @Test
  public void testStructOfStruct() throws Exception {
    testConverter(new DataGenerators.StructOfStruct());
  }

  @Test
  public void testArrayOfPrimitive() throws Exception {
    testConverter(new DataGenerators.ArrayOfPrimitive());
  }

  @Test
  public void testArrayOfArray() throws Exception {
    testConverter(new DataGenerators.ArrayOfArray());
  }

  @Test
  public void testArrayOfMap() throws Exception {
    testConverter(new DataGenerators.ArrayOfMap());
  }

  @Test
  public void testArrayOfStruct() throws Exception {
    testConverter(new DataGenerators.ArrayOfStruct());
  }

  @Test
  public void testMapOfPrimitives() throws Exception {
    testConverter(new DataGenerators.MapOfPrimitives());
  }

  @Test
  public void testMapOfArray() throws Exception {
    testConverter(new DataGenerators.MapOfArray());
  }

  @Test
  public void testMapOfMap() throws Exception {
    testConverter(new DataGenerators.MapOfMap());
  }

  @Test
  public void testMapOfStruct() throws Exception {
    testConverter(new DataGenerators.MapOfStruct());
  }
}
