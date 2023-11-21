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
package org.apache.iceberg.aliyun.oss.mock;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import org.apache.iceberg.aliyun.oss.AliyunOSSExtension;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class AliyunOSSMockExtension implements AliyunOSSExtension {

  private final Map<String, Object> properties;

  private AliyunOSSMockApp ossMockApp;

  private AliyunOSSMockExtension(Map<String, Object> properties) {
    this.properties = properties;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String keyPrefix() {
    return "mock-objects/";
  }

  @Override
  public void start() {
    ossMockApp = AliyunOSSMockApp.start(properties);
  }

  @Override
  public void stop() {
    ossMockApp.stop();
  }

  @Override
  public OSS createOSSClient() {
    String endpoint =
        String.format(
            "http://localhost:%s",
            properties.getOrDefault(
                AliyunOSSMockApp.PROP_HTTP_PORT, AliyunOSSMockApp.PORT_HTTP_PORT_DEFAULT));
    return new OSSClientBuilder().build(endpoint, "foo", "bar");
  }

  private File rootDir() {
    Object rootDir = properties.get(AliyunOSSMockApp.PROP_ROOT_DIR);
    Preconditions.checkNotNull(rootDir, "Root directory cannot be null");
    return new File(rootDir.toString());
  }

  @Override
  public void setUpBucket(String bucket) {
    createOSSClient().createBucket(bucket);
  }

  @Override
  public void tearDownBucket(String bucket) {
    try {
      Files.walk(rootDir().toPath())
          .filter(p -> p.toFile().isFile())
          .forEach(
              p -> {
                try {
                  Files.delete(p);
                } catch (IOException e) {
                  // delete this file quietly.
                }
              });

      createOSSClient().deleteBucket(bucket);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static class Builder {
    private Map<String, Object> props = Maps.newHashMap();

    public Builder silent() {
      props.put(AliyunOSSMockApp.PROP_SILENT, true);
      return this;
    }

    public AliyunOSSExtension build() {
      String rootDir = (String) props.get(AliyunOSSMockApp.PROP_ROOT_DIR);
      if (Strings.isNullOrEmpty(rootDir)) {
        File dir =
            new File(
                System.getProperty("java.io.tmpdir"),
                "oss-mock-file-store-" + System.currentTimeMillis());
        rootDir = dir.getAbsolutePath();
        props.put(AliyunOSSMockApp.PROP_ROOT_DIR, rootDir);
      }
      File root = new File(rootDir);
      root.deleteOnExit();
      root.mkdir();
      return new AliyunOSSMockExtension(props);
    }
  }
}
