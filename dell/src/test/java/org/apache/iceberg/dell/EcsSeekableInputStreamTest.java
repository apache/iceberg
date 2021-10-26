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

public class EcsSeekableInputStreamTest {

  @Rule
  public EcsS3MockRule rule = EcsS3MockRule.create();

  @Test
  public void generalTest() throws IOException {
    String objectName = "test";
    rule.getClient().putObject(new PutObjectRequest(rule.getBucket(), objectName, "1234567890".getBytes()));

    try (EcsSeekableInputStream input = new EcsSeekableInputStream(
        rule.getClient(),
        new EcsURI(rule.getBucket(), objectName))) {
      // read one byte
      assertEquals("1 at 0", '1', input.read());

      // read bytes
      byte[] bytes = new byte[3];
      assertEquals("read 3 bytes", 3, input.read(bytes));
      assertArrayEquals("234 at 1-3", new byte[] {'2', '3', '4'}, bytes);

      // jump
      input.seek(2);
      assertEquals("3 at 2", '3', input.read());

      // jump to invalid pos won't cause exception
      input.seek(100);
      input.seek(9);
      assertEquals("0 at 9", '0', input.read());
    }
  }
}
