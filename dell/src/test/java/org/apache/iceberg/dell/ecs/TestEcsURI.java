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
package org.apache.iceberg.dell.ecs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.Test;

public class TestEcsURI {

  @Test
  public void testConstructor() {
    assertURI("bucket", "", new EcsURI("ecs://bucket"));
    assertURI("bucket", "", new EcsURI("ecs://bucket/"));
    assertURI("bucket", "", new EcsURI("ecs://bucket//"));
    assertURI("bucket", "a", new EcsURI("ecs://bucket//a"));
    assertURI("bucket", "a/b", new EcsURI("ecs://bucket/a/b"));
    assertURI("bucket", "a//b", new EcsURI("ecs://bucket/a//b"));
    assertURI("bucket", "a//b", new EcsURI("ecs://bucket//a//b"));
  }

  @Test
  public void testConstructorWithBucketAndName() {
    assertURI("bucket", "", new EcsURI("bucket", ""));
    assertURI("bucket", "", new EcsURI("bucket", "/"));
    assertURI("bucket", "", new EcsURI("bucket", "//"));
    assertURI("bucket", "a", new EcsURI("bucket", "a"));
    assertURI("bucket", "a", new EcsURI("bucket", "/a"));
    assertURI("bucket", "a/b", new EcsURI("bucket", "a/b"));
    assertURI("bucket", "a//b", new EcsURI("bucket", "/a//b"));
  }

  private void assertURI(String bucket, String name, EcsURI ecsURI) {
    assertThat(ecsURI.bucket()).as("bucket").isEqualTo(bucket);
    assertThat(ecsURI.name()).as("name").isEqualTo(name);
  }

  @Test
  public void testInvalidLocation() {
    assertThatThrownBy(() -> new EcsURI("http://bucket/a"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Invalid ecs location: http://bucket/a");
  }
}
