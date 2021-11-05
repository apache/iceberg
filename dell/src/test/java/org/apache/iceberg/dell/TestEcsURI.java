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

package org.apache.iceberg.dell;

import org.apache.iceberg.exceptions.ValidationException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class TestEcsURI {

  @Test
  public void testCreate() {
    assertEquals(
        new EcsURI("bucket", ""),
        EcsURI.create("ecs://bucket"));
    assertEquals(
        new EcsURI("bucket", ""),
        EcsURI.create("ecs://bucket/"));
    assertEquals(
        new EcsURI("bucket", ""),
        EcsURI.create("ecs://bucket//"));
    assertEquals(
        new EcsURI("bucket", "a"),
        EcsURI.create("ecs://bucket//a"));
    assertEquals(
        new EcsURI("bucket", "a/b"),
        EcsURI.create("ecs://bucket/a/b"));
    assertEquals(
        new EcsURI("bucket", "a//b"),
        EcsURI.create("ecs://bucket/a//b"));
    assertEquals(
        new EcsURI("bucket", "a//b"),
        EcsURI.create("ecs://bucket//a//b"));
  }

  @Test
  public void testInvalidLocation() {
    assertThrows(
        ValidationException.class,
        () -> EcsURI.create("http://bucket/a"));
  }
}
