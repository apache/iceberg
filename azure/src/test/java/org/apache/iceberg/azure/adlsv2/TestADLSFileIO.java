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
package org.apache.iceberg.azure.adlsv2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SerializableSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestADLSFileIO {

  @Test
  public void testConstructorWithClientSupplier() {
    DataLakeFileSystemClient mockClient = mock(DataLakeFileSystemClient.class);
    SerializableSupplier<DataLakeFileSystemClient> supplier = () -> mockClient;

    ADLSFileIO fileIO = new ADLSFileIO(supplier);

    // Verify properties are initialized (should not throw NPE)
    assertThat(fileIO.properties()).isNotNull();
    assertThat(fileIO.properties()).isEmpty();
  }

  @Test
  public void testConstructorWithClientSupplierAndInitialize() {
    DataLakeFileSystemClient mockClient = mock(DataLakeFileSystemClient.class);
    SerializableSupplier<DataLakeFileSystemClient> supplier = () -> mockClient;

    ADLSFileIO fileIO = new ADLSFileIO(supplier);
    fileIO.initialize(ImmutableMap.of("key1", "value1"));

    // Verify properties from initialize are used
    assertThat(fileIO.properties()).containsEntry("key1", "value1");
  }

  @Test
  public void testClientSupplierIsUsed() {
    DataLakeFileSystemClient mockClient = mock(DataLakeFileSystemClient.class);

    SerializableSupplier<DataLakeFileSystemClient> supplier = () -> mockClient;

    ADLSFileIO fileIO = new ADLSFileIO(supplier);
    fileIO.initialize(ImmutableMap.of());

    // Call client method to verify supplier is invoked
    DataLakeFileSystemClient client =
        fileIO.client("abfs://container@account.dfs.core.windows.net/path/to/file");

    assertThat(client).isEqualTo(mockClient);
  }

  @Test
  public void testClientSupplierWithoutInitialize() {
    DataLakeFileSystemClient mockClient = mock(DataLakeFileSystemClient.class);
    DataLakeFileClient mockFileClient = mock(DataLakeFileClient.class);

    when(mockClient.getFileClient("path/to/file")).thenReturn(mockFileClient);

    SerializableSupplier<DataLakeFileSystemClient> supplier = () -> mockClient;

    ADLSFileIO fileIO = new ADLSFileIO(supplier);

    // Should work without calling initialize()
    // This verifies azureProperties is initialized in constructor
    assertThat(fileIO.properties()).isNotNull();
    assertThat(fileIO.properties()).isEmpty();

    // Should be able to create files without NPE
    InputFile inputFile =
        fileIO.newInputFile("abfs://container@account.dfs.core.windows.net/path/to/file");
    assertThat(inputFile).isNotNull();
  }

  @Test
  public void testNoArgConstructor() {
    ADLSFileIO fileIO = new ADLSFileIO();

    // Properties should be null before initialization
    // Initialize with empty map to avoid NPE
    fileIO.initialize(ImmutableMap.of());

    assertThat(fileIO.properties()).isNotNull();
    assertThat(fileIO.properties()).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void testSerializationWithClientSupplier(
      TestHelpers.RoundTripSerializer<FileIO> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    // Use an AtomicInteger to track supplier invocations across serialization
    AtomicInteger supplierInvocationCount = new AtomicInteger(0);

    SerializableSupplier<DataLakeFileSystemClient> supplier =
        () -> {
          supplierInvocationCount.incrementAndGet();
          // Return null - we're only testing serialization, not actual client usage
          return null;
        };

    ADLSFileIO fileIO = new ADLSFileIO(supplier);
    fileIO.initialize(ImmutableMap.of("key1", "value1", "key2", "value2"));

    // Verify original FileIO works
    assertThat(fileIO.properties()).containsEntry("key1", "value1");
    assertThat(fileIO.properties()).containsEntry("key2", "value2");

    // Serialize and deserialize
    FileIO deserializedFileIO = roundTripSerializer.apply(fileIO);

    // Verify properties are preserved after deserialization
    assertThat(deserializedFileIO.properties()).isEqualTo(fileIO.properties());

    // Verify the deserialized FileIO is an ADLSFileIO and can call client()
    assertThat(deserializedFileIO).isInstanceOf(ADLSFileIO.class);
    ADLSFileIO deserializedADLSFileIO = (ADLSFileIO) deserializedFileIO;

    // Call client() to verify the supplier was serialized and can be invoked
    DataLakeFileSystemClient client =
        deserializedADLSFileIO.client("abfs://container@account.dfs.core.windows.net/path");
    // The supplier returns null, so client should be null
    assertThat(client).isNull();
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void testSerializationWithNoArgConstructor(
      TestHelpers.RoundTripSerializer<FileIO> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    ADLSFileIO fileIO = new ADLSFileIO();
    fileIO.initialize(ImmutableMap.of("key1", "value1", "key2", "value2"));

    // Serialize and deserialize
    FileIO deserializedFileIO = roundTripSerializer.apply(fileIO);

    // Verify properties are preserved after deserialization
    assertThat(deserializedFileIO.properties()).isEqualTo(fileIO.properties());
  }
}
