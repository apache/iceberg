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
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class OSSMockRule implements TestRule {

  private final Map<String, Object> properties;

  private OSSMockApplication ossMockApp;

  private OSSMockRule(Map<String, Object> properties) {
    this.properties = properties;
  }

  private void start() {
    ossMockApp = OSSMockApplication.start(properties);
  }

  private void stop() {
    ossMockApp.stop();
  }

  public OSS createOSSClient() {
    String endpoint = String.format("http://localhost:%s", properties.getOrDefault(OSSMockApplication.PROP_HTTP_PORT,
        OSSMockApplication.PORT_HTTP_PORT_DEFAULT));
    return new OSSClientBuilder().build(endpoint, "foo", "bar");
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        start();
        try {
          base.evaluate();
        } finally {
          stop();
        }
      }
    };
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Map<String, Object> props = Maps.newHashMap();

    public Builder withRootDir(String rootDir) {
      props.put(OSSMockApplication.PROP_ROOT_DIR, rootDir);
      return this;
    }

    public Builder withHttpPort(int httpPort) {
      props.put(OSSMockApplication.PROP_HTTP_PORT, httpPort);
      return this;
    }

    public Builder silent() {
      props.put(OSSMockApplication.PROP_SILENT, true);
      return this;
    }

    public OSSMockRule build() {
      return new OSSMockRule(props);
    }
  }
}
