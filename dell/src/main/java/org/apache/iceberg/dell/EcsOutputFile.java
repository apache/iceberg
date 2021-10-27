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

import com.emc.object.s3.S3Client;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

public class EcsOutputFile implements OutputFile {

  private final S3Client client;
  private final String location;
  private final EcsURI uri;

  public EcsOutputFile(S3Client client, String location) {
    this.client = client;
    this.location = location;
    this.uri = LocationUtils.checkAndParseLocation(location);
  }

  /**
   * Check object existence and then create a {@link PositionOutputStream}
   *
   * @return Output stream of object
   */
  @Override
  public PositionOutputStream create() {
    if (!toInputFile().exists()) {
      return createOrOverwrite();
    } else {
      throw new AlreadyExistsException("The object is existed, %s", uri);
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    // use built-in 1 KiB byte buffer
    return new EcsAppendOutputStream(client, uri, new byte[1_000]);
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public InputFile toInputFile() {
    return new EcsInputFile(client, location);
  }
}
