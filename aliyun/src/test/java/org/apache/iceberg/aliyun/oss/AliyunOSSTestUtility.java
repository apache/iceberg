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

package org.apache.iceberg.aliyun.oss;

import org.apache.iceberg.aliyun.oss.mock.AliyunOSSMockRule;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AliyunOSSTestUtility {
  private static final Logger LOG = LoggerFactory.getLogger(AliyunOSSTestUtility.class);
  private static final String ALIYUN_OSS_TEST_RULE_CLASS = "ALIYUN_OSS_TEST_RULE_CLASS";

  private AliyunOSSTestUtility() {
  }

  public static AliyunOSSTestRule initialize() {
    String implClass = System.getenv(ALIYUN_OSS_TEST_RULE_CLASS);
    AliyunOSSTestRule testRule;

    if (!Strings.isNullOrEmpty(implClass)) {
      DynConstructors.Ctor<AliyunOSSTestRule> ctor;
      LOG.info("The initializing AliyunOSSTestRule implementation is: {}", implClass);
      try {
        ctor = DynConstructors.builder(AliyunOSSTestRule.class).impl(implClass).buildChecked();
        testRule = ctor.newInstance();
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(String.format(
            "Cannot initialize AliyunOSSTestRule, missing no-arg constructor: %s", implClass), e);
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            String.format("Cannot initialize AliyunOSSTestRule, %s does not implement it.", implClass), e);
      }
    } else {
      testRule = AliyunOSSMockRule.builder().silent().build();
    }

    return testRule;
  }
}
