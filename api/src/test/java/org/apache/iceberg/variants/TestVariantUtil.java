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
package org.apache.iceberg.variants;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

public class TestVariantUtil {
  @Test
  public void testReadByteUnsigned() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {(byte) 0xFF});
    assertThat(VariantUtil.readByte(buffer, 0)).isEqualTo(255);
  }

  @Test
  public void testRead2ByteUnsigned() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF});
    assertThat(VariantUtil.readLittleEndianUnsigned(buffer, 0, 2)).isEqualTo(65535);
  }

  @Test
  public void testRead3ByteUnsigned() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    assertThat(VariantUtil.readLittleEndianUnsigned(buffer, 0, 3)).isEqualTo(16777215);
  }
}
