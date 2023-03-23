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
package org.apache.iceberg.inmemory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestInMemoryOutputFile {
  @Test
  public void testWriteAfterClose() throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    OutputStream outputStream = outputFile.create();
    outputStream.write('a');
    outputStream.write('b');
    outputStream.close();
    Assertions.assertThatThrownBy(() -> outputStream.write('c')).hasMessage("Stream is closed");
    Assertions.assertThat(outputFile.toByteArray())
        .isEqualTo("ab".getBytes(StandardCharsets.ISO_8859_1));
  }
}
