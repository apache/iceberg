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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.jupiter.api.Test;

public class TestTry {

  @Test
  public void testSuccessfulOperation() throws Exception {
    Try<String> result = Try.capture(() -> "success");

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.isFailure()).isFalse();
    assertThat(result.get()).isEqualTo("success");
    assertThat(result.orElse("default")).isEqualTo("success");
    assertThat(result.getOrThrow()).isEqualTo("success");
  }

  @Test
  public void testFailedOperation() {
    Exception testException = new IllegalArgumentException("test exception");
    Try<String> result =
        Try.capture(
            () -> {
              throw testException;
            });

    assertThat(result.isSuccess()).isFalse();
    assertThat(result.isFailure()).isTrue();
    assertThat(result.orElse("default")).isEqualTo("default");
  }

  @Test
  public void testGetWithFailure() {
    Exception testException = new IllegalArgumentException("test exception");
    Try<String> result =
        Try.capture(
            () -> {
              throw testException;
            });

    assertThatThrownBy(result::get).isSameAs(testException).hasMessage("test exception");
  }

  @Test
  public void testCheckedException() {
    Try<String> result =
        Try.capture(
            () -> {
              throw new Exception("checked exception");
            });

    assertThat(result.isFailure()).isTrue();
    assertThatThrownBy(result::get).isInstanceOf(Exception.class).hasMessage("checked exception");
    assertThatThrownBy(result::getOrThrow)
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(Exception.class)
        .hasRootCauseMessage("checked exception");
  }

  @Test
  public void testRuntimeException() {
    RuntimeException runtimeException = new RuntimeException("runtime exception");
    Try<String> result =
        Try.capture(
            () -> {
              throw runtimeException;
            });

    assertThat(result.isFailure()).isTrue();
    assertThatThrownBy(result::get).isSameAs(runtimeException).hasMessage("runtime exception");
    assertThatThrownBy(result::getOrThrow)
        .isSameAs(runtimeException)
        .hasMessage("runtime exception");
  }

  @Test
  public void testNullValue() throws Exception {
    Try<String> result = Try.capture(() -> null);

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.get()).isNull();
    assertThat(result.orElse("default")).isNull();
    assertThat(result.getOrThrow()).isNull();
  }

  @Test
  public void testOrElseWithDefaultValue() {
    Try<String> result =
        Try.capture(
            () -> {
              throw new RuntimeException("test exception");
            });

    assertThat(result.orElse("default value")).isEqualTo("default value");
  }

  @Test
  public void testSerialization() throws Exception {
    Try<String> original = Try.capture(() -> "serialized value");

    // Serialize to byte array
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bytes);
    out.writeObject(original);
    out.close();

    // Deserialize from byte array
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()));
    Try<String> deserialized = (Try<String>) in.readObject();
    in.close();

    // Verify state is preserved
    assertThat(deserialized.isSuccess()).isTrue();
    assertThat(deserialized.get()).isEqualTo("serialized value");
    assertThat(deserialized.getOrThrow()).isEqualTo("serialized value");
  }

  @Test
  public void testSerializationWithException() throws Exception {
    Exception originalException = new IllegalArgumentException("test exception");
    Try<String> original =
        Try.capture(
            () -> {
              throw originalException;
            });
    // Serialize to byte array
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bytes);
    out.writeObject(original);
    out.close();

    // Deserialize from byte array
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()));
    Try<String> deserialized = (Try<String>) in.readObject();
    in.close();

    // Verify state is preserved
    assertThat(deserialized.isFailure()).isTrue();
    assertThatThrownBy(deserialized::get)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("test exception");
    assertThatThrownBy(deserialized::getOrThrow)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("test exception");
  }
}
