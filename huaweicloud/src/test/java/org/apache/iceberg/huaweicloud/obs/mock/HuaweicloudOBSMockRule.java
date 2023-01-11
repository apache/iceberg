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
package org.apache.iceberg.huaweicloud.obs.mock;

import com.obs.services.IObsClient;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.huaweicloud.obs.HuaweicloudOBSTestRule;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class HuaweicloudOBSMockRule implements HuaweicloudOBSTestRule {

  private final Map<String, Object> properties;

  private HuaweicloudOBSMockApp obsMockApp;

  private HuaweicloudOBSMockRule(Map<String, Object> properties) {
    this.properties = properties;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String keyPrefix() {
    return "mock-objects-";
  }

  @Override
  public void start() {
    obsMockApp = HuaweicloudOBSMockApp.start(properties);
  }

  @Override
  public void stop() {
    obsMockApp.stop();
  }

  @Override
  public IObsClient createOBSClient() {
    String endpoint =
        String.format(
            "http://localhost:%s",
            properties.getOrDefault(
                HuaweicloudOBSMockApp.PROP_HTTP_PORT,
                HuaweicloudOBSMockApp.PORT_HTTP_PORT_DEFAULT));
    ObsConfiguration config = new ObsConfiguration();
    config.setEndPoint(endpoint);
    config.setPathStyle(true);
    return new ObsClient("foo", "bar", config);
  }

  private File rootDir() {
    Object rootDir = properties.get(HuaweicloudOBSMockApp.PROP_ROOT_DIR);
    Preconditions.checkNotNull(rootDir, "Root directory cannot be null");
    return new File(rootDir.toString());
  }

  @Override
  public void setUpBucket(String bucket) {
    createOBSClient().createBucket(bucket);
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

      createOBSClient().deleteBucket(bucket);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static class Builder {
    private Map<String, Object> props = Maps.newHashMap();

    public Builder silent() {
      props.put(HuaweicloudOBSMockApp.PROP_SILENT, true);
      return this;
    }

    public HuaweicloudOBSTestRule build() {
      String rootDir = (String) props.get(HuaweicloudOBSMockApp.PROP_ROOT_DIR);
      if (Strings.isNullOrEmpty(rootDir)) {
        File dir =
            new File(
                FileUtils.getTempDirectory(), "obs-mock-file-store-" + System.currentTimeMillis());
        rootDir = dir.getAbsolutePath();
        props.put(HuaweicloudOBSMockApp.PROP_ROOT_DIR, rootDir);
      }
      File root = new File(rootDir);
      root.deleteOnExit();
      root.mkdir();
      return new HuaweicloudOBSMockRule(props);
    }
  }
}
