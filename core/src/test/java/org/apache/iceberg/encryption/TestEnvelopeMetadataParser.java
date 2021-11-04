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

import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;

public class TestEnvelopeMetadataParser {

  @Test
  public void testParser() {
    String kekId = "kek1";
    String wrappedDek = "WrappedDek";
    EncryptionAlgorithm algo = EncryptionAlgorithm.AES_GCM;
    EnvelopeMetadata metadata = new EnvelopeMetadata(kekId, wrappedDek, algo);
    ByteBuffer serialized = EnvelopeMetadataParser.toJson(metadata);

    EnvelopeMetadata parsedMetadata = EnvelopeMetadataParser.fromJson(serialized);
    Assert.assertEquals(parsedMetadata.kekId(), kekId);
    Assert.assertEquals(parsedMetadata.wrappedDek(), wrappedDek);
    Assert.assertEquals(parsedMetadata.algorithm(), algo);
    // Also test object comparison
    Assert.assertEquals(parsedMetadata, metadata);
  }
}
