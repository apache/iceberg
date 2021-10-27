/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.dell;

import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.dell.mock.EcsS3MockRule;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class EcsFileTest {

  @Rule
  public EcsS3MockRule rule = EcsS3MockRule.create();

  @Test
  public void generalTest() throws IOException {
    String objectName = "test";
    String location = new EcsURI(rule.getBucket(), objectName).toString();
    EcsInputFile inputFile = new EcsInputFile(rule.getClient(), location);
    EcsOutputFile outpufFile = new EcsOutputFile(rule.getClient(), location);

    // absent
    assertFalse("file is absent", inputFile.exists());
    assertEquals("file length is 0 if absent", 0, inputFile.getLength());

    // write and read
    try (PositionOutputStream output = outpufFile.create()) {
      output.write("1234567890".getBytes());
    }

    assertTrue("file is present", inputFile.exists());
    assertEquals("file length is 10", 10, inputFile.getLength());
    try (SeekableInputStream input = inputFile.newStream()) {
      assertEquals("file content", "1234567890",
          IOUtils.toString(input));
    }

    // rewrite file
    try (PositionOutputStream output = outpufFile.createOrOverwrite()) {
      output.write("987654321".getBytes());
    }

    assertEquals("new file length is 9", 9, inputFile.getLength());
    try (SeekableInputStream input = inputFile.newStream()) {
      assertEquals("new file content", "987654321",
          IOUtils.toString(input));
    }

    // write checker
    assertThrows("If file exists, throw exception", AlreadyExistsException.class, outpufFile::create);
  }
}
