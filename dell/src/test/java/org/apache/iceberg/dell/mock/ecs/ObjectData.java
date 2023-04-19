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
package org.apache.iceberg.dell.mock.ecs;

import com.emc.object.Range;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.shadow.org.apache.commons.codec.digest.DigestUtils;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/** Object data in memory. */
public class ObjectData {
  public final byte[] content;
  public final Map<String, String> userMetadata;

  public static ObjectData create(byte[] content, S3ObjectMetadata metadata) {
    Map<String, String> userMetadata = new LinkedHashMap<>();
    if (metadata != null) {
      userMetadata.putAll(metadata.getUserMetadata());
    }

    return new ObjectData(content, userMetadata);
  }

  private ObjectData(byte[] content, Map<String, String> userMetadata) {
    this.content = content;
    this.userMetadata = userMetadata;
  }

  public int length() {
    return content.length;
  }

  public ObjectData appendContent(byte[] appendedData) {
    byte[] newContent = Arrays.copyOf(content, content.length + appendedData.length);
    System.arraycopy(appendedData, 0, newContent, content.length, appendedData.length);
    return new ObjectData(newContent, userMetadata);
  }

  public InputStream createInputStream(Range range) {
    int offset = range.getFirst().intValue();
    int length;
    if (range.getLast() != null) {
      length = range.getLast().intValue() - offset;
    } else {
      length = content.length - offset;
    }

    return new ByteArrayInputStream(content, offset, length);
  }

  public S3ObjectMetadata createFullMetadata() {
    S3ObjectMetadata metadata = new S3ObjectMetadata();
    metadata.setETag(DigestUtils.md5Hex(content));
    metadata.setContentLength((long) content.length);
    metadata.setUserMetadata(userMetadata);
    return metadata;
  }
}
