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

import com.emc.object.s3.request.PutObjectRequest;
import java.io.IOException;
import org.apache.iceberg.dell.mock.EcsS3MockRule;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestEcsSeekableInputStream {

  @Rule
  public EcsS3MockRule rule = EcsS3MockRule.create();

  @Test
  public void testSeekPosRead() throws IOException {
    String objectName = "test";
    rule.getClient().putObject(new PutObjectRequest(rule.getBucket(), objectName, "0123456789".getBytes()));

    try (EcsSeekableInputStream input = new EcsSeekableInputStream(
        rule.getClient(),
        new EcsURI(rule.getBucket(), objectName))) {
      // read one byte
      assertEquals("Read 0 without additional pos", '0', input.read());

      // read bytes
      byte[] bytes = new byte[3];
      assertEquals("Read 3 bytes", 3, input.read(bytes));
      assertArrayEquals("Read next 123 after 0", new byte[] {'1', '2', '3'}, bytes);

      // jump
      input.seek(2);
      assertEquals("Read 2 when seek to 2", '2', input.read());

      // jump to invalid pos won't cause exception
      input.seek(100);
      input.seek(9);
      assertEquals("Read 9 that is the final pos", '9', input.read());
    }
  }
}
