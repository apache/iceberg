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
package org.apache.iceberg.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestTryUtil {

  @Test
  public void testSuccessfulOperation() throws Exception {
    TryUtil.Try<String> result = TryUtil.run(() -> "success");

    Assertions.assertThat(result.isSuccess()).isTrue();
    Assertions.assertThat(result.isFailure()).isFalse();
    Assertions.assertThat(result.get()).isEqualTo("success");
    Assertions.assertThat(result.orElse("default")).isEqualTo("success");
  }

  @Test
  public void testFailedOperation() {
    Exception testException = new IllegalArgumentException("test exception");
    TryUtil.Try<String> result =
        TryUtil.run(
            () -> {
              throw testException;
            });

    Assertions.assertThat(result.isSuccess()).isFalse();
    Assertions.assertThat(result.isFailure()).isTrue();
    Assertions.assertThat(result.orElse("default")).isEqualTo("default");
  }

  @Test
  public void testGetWithFailure() {
    Exception testException = new IllegalArgumentException("test exception");
    TryUtil.Try<String> result =
        TryUtil.run(
            () -> {
              throw testException;
            });

    Assertions.assertThatThrownBy(result::get).isSameAs(testException);
  }

  @Test
  public void testCheckedException() {
    TryUtil.Try<String> result =
        TryUtil.run(
            () -> {
              throw new Exception("checked exception");
            });

    Assertions.assertThat(result.isFailure()).isTrue();
    Assertions.assertThatThrownBy(result::get)
        .isInstanceOf(Exception.class)
        .hasMessage("checked exception");
  }

  @Test
  public void testRuntimeException() {
    RuntimeException runtimeException = new RuntimeException("runtime exception");
    TryUtil.Try<String> result =
        TryUtil.run(
            () -> {
              throw runtimeException;
            });

    Assertions.assertThat(result.isFailure()).isTrue();
    Assertions.assertThatThrownBy(result::get).isSameAs(runtimeException);
  }

  @Test
  public void testNullValue() throws Exception {
    TryUtil.Try<String> result = TryUtil.run(() -> null);

    Assertions.assertThat(result.isSuccess()).isTrue();
    Assertions.assertThat(result.get()).isNull();
    Assertions.assertThat(result.orElse("default")).isNull();
  }

  @Test
  public void testOrElseWithDefaultValue() {
    TryUtil.Try<String> result =
        TryUtil.run(
            () -> {
              throw new RuntimeException("test exception");
            });

    Assertions.assertThat(result.orElse("default value")).isEqualTo("default value");
  }

  @Test
  public void testSerialization() throws Exception {
    TryUtil.Try<String> original = TryUtil.run(() -> "serialized value");

    // Serialize to byte array
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bytes);
    out.writeObject(original);
    out.close();

    // Deserialize from byte array
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()));
    TryUtil.Try<String> deserialized = (TryUtil.Try<String>) in.readObject();
    in.close();

    // Verify state is preserved
    Assertions.assertThat(deserialized.isSuccess()).isTrue();
    Assertions.assertThat(deserialized.get()).isEqualTo("serialized value");
  }

  @Test
  public void testSerializationWithException() throws Exception {
    Exception original_exception = new IllegalArgumentException("test exception");
    TryUtil.Try<String> original =
        TryUtil.run(
            () -> {
              throw original_exception;
            });
    // Serialize to byte array
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bytes);
    out.writeObject(original);
    out.close();

    // Deserialize from byte array
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()));
    TryUtil.Try<String> deserialized = (TryUtil.Try<String>) in.readObject();
    in.close();

    // Verify state is preserved
    Assertions.assertThat(deserialized.isFailure()).isTrue();
    Assertions.assertThatThrownBy(deserialized::get)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("test exception");
  }
}
