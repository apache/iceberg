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
package org.apache.iceberg.encryption;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.junit.jupiter.api.Test;

public class TestEncryptingFileIO {

  @Test
  public void delegateEncryptingIOWithAndWithoutMixins() {
    EncryptionManager em = mock(EncryptionManager.class);

    FileIO fileIONoMixins = mock(FileIO.class);
    assertThat(EncryptingFileIO.combine(fileIONoMixins, em))
        .isInstanceOf(EncryptingFileIO.class)
        .isNotInstanceOf(EncryptingFileIO.WithSupportsPrefixOperations.class)
        .isNotInstanceOf(EncryptingFileIO.WithDelegateFileIO.class)
        .extracting(EncryptingFileIO::encryptionManager)
        .isEqualTo(em);

    FileIO fileIOWithPrefixOps =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsPrefixOperations.class));
    assertThat(EncryptingFileIO.combine(fileIOWithPrefixOps, em))
        .isInstanceOf(EncryptingFileIO.WithSupportsPrefixOperations.class)
        .extracting(EncryptingFileIO::encryptionManager)
        .isEqualTo(em);

    DelegateFileIO delegateFileIO = mock(DelegateFileIO.class);
    assertThat(EncryptingFileIO.combine(delegateFileIO, em))
        .isInstanceOf(EncryptingFileIO.WithDelegateFileIO.class)
        .isInstanceOf(DelegateFileIO.class)
        .isInstanceOf(SupportsBulkOperations.class)
        .extracting(EncryptingFileIO::encryptionManager)
        .isEqualTo(em);
  }

  @Test
  public void prefixOperationsDelegation() {
    EncryptionManager em = mock(EncryptionManager.class);
    SupportsPrefixOperations delegate = mock(SupportsPrefixOperations.class);

    EncryptingFileIO.WithSupportsPrefixOperations fileIO =
        (EncryptingFileIO.WithSupportsPrefixOperations) EncryptingFileIO.combine(delegate, em);

    String prefix = "prefix";
    Iterable<FileInfo> fileInfos = mock(Iterable.class);
    when(delegate.listPrefix(prefix)).thenReturn(fileInfos);
    assertThat(fileIO.listPrefix(prefix)).isEqualTo(fileInfos);

    fileIO.deletePrefix(prefix);
    verify(delegate).deletePrefix(prefix);
  }

  @Test
  public void reWrappingDelegateFileIOPreservesType() {
    EncryptionManager em1 = mock(EncryptionManager.class);
    EncryptionManager em2 = mock(EncryptionManager.class);
    DelegateFileIO delegate = mock(DelegateFileIO.class);

    EncryptingFileIO first = EncryptingFileIO.combine(delegate, em1);
    EncryptingFileIO second = EncryptingFileIO.combine(first, em2);
    assertThat(second)
        .isInstanceOf(EncryptingFileIO.WithDelegateFileIO.class)
        .extracting(EncryptingFileIO::encryptionManager)
        .isEqualTo(em2);
  }

  @Test
  public void delegateFileIODelegation() {
    EncryptionManager em = mock(EncryptionManager.class);
    DelegateFileIO delegate = mock(DelegateFileIO.class);

    EncryptingFileIO.WithDelegateFileIO fileIO =
        (EncryptingFileIO.WithDelegateFileIO) EncryptingFileIO.combine(delegate, em);

    String prefix = "prefix";
    Iterable<FileInfo> fileInfos = mock(Iterable.class);
    when(delegate.listPrefix(prefix)).thenReturn(fileInfos);
    assertThat(fileIO.listPrefix(prefix)).isEqualTo(fileInfos);

    fileIO.deletePrefix(prefix);
    verify(delegate).deletePrefix(prefix);

    List<String> pathsToDelete = List.of("path1", "path2");
    fileIO.deleteFiles(pathsToDelete);
    verify(delegate).deleteFiles(pathsToDelete);
  }

  @Test
  public void closeClosesUnderlyingFileIO() {
    EncryptionManager em = mock(EncryptionManager.class);

    FileIO fileIONoMixins = mock(FileIO.class);
    EncryptingFileIO.combine(fileIONoMixins, em).close();
    verify(fileIONoMixins).close();

    SupportsPrefixOperations prefixIO = mock(SupportsPrefixOperations.class);
    EncryptingFileIO prefixEncryptingIO = EncryptingFileIO.combine(prefixIO, em);
    assertThat(prefixEncryptingIO)
        .isInstanceOf(EncryptingFileIO.WithSupportsPrefixOperations.class);
    prefixEncryptingIO.close();
    verify(prefixIO).close();

    DelegateFileIO delegate = mock(DelegateFileIO.class);
    EncryptingFileIO delegateEncryptingIO = EncryptingFileIO.combine(delegate, em);
    assertThat(delegateEncryptingIO).isInstanceOf(EncryptingFileIO.WithDelegateFileIO.class);
    delegateEncryptingIO.close();
    verify(delegate).close();
  }

  @Test
  public void closeClosesEncryptionManagerWhenCloseable() throws Exception {
    EncryptionManager em =
        mock(EncryptionManager.class, withSettings().extraInterfaces(Closeable.class));
    DelegateFileIO delegate = mock(DelegateFileIO.class);

    EncryptingFileIO fileIO = EncryptingFileIO.combine(delegate, em);
    fileIO.close();

    verify(delegate).close();
    verify((Closeable) em).close();
  }

  @Test
  public void properties() {
    EncryptionManager em = mock(EncryptionManager.class);
    FileIO io = mock(FileIO.class);
    when(io.properties()).thenReturn(Map.of("key", "value"));

    assertThat(EncryptingFileIO.combine(io, em).properties())
        .containsExactly(Map.entry("key", "value"));
  }
}
