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

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.Assert;
import org.junit.Test;

public class TestEcsURI {

  @Test
  public void testCreate() {
    Assert.assertEquals(
        new EcsURI("bucket", ""),
        EcsURI.create("ecs://bucket"));
    Assert.assertEquals(
        new EcsURI("bucket", ""),
        EcsURI.create("ecs://bucket/"));
    Assert.assertEquals(
        new EcsURI("bucket", ""),
        EcsURI.create("ecs://bucket//"));
    Assert.assertEquals(
        new EcsURI("bucket", "a"),
        EcsURI.create("ecs://bucket//a"));
    Assert.assertEquals(
        new EcsURI("bucket", "a/b"),
        EcsURI.create("ecs://bucket/a/b"));
    Assert.assertEquals(
        new EcsURI("bucket", "a//b"),
        EcsURI.create("ecs://bucket/a//b"));
    Assert.assertEquals(
        new EcsURI("bucket", "a//b"),
        EcsURI.create("ecs://bucket//a//b"));
  }

  @Test
  public void testInvalidLocation() {
    AssertHelpers.assertThrows(
        "Invalid location should cause exception",
        ValidationException.class,
        "http://bucket/a",
        () -> EcsURI.create("http://bucket/a"));
  }
}
