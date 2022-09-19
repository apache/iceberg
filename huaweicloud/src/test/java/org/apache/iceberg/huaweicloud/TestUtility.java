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
package org.apache.iceberg.huaweicloud;

import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.huaweicloud.obs.HuaweicloudOBSTestRule;
import org.apache.iceberg.huaweicloud.obs.OBSURI;
import org.apache.iceberg.huaweicloud.obs.mock.HuaweicloudOBSMockRule;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtility {
  private static final Logger LOG = LoggerFactory.getLogger(TestUtility.class);

  // System environment variables for Huaweicloud Access Key Pair.
  private static final String HUAWEICLOUD_TEST_ACCESS_KEY_ID = "HUAWEICLOUD_TEST_ACCESS_KEY_ID";
  private static final String HUAWEICLOUD_TEST_ACCESS_KEY_SECRET =
      "HUAWEICLOUD_TEST_ACCESS_KEY_SECRET";

  // System environment variables for Huaweicloud OBS
  private static final String HUAWEICLOUD_TEST_OBS_RULE_CLASS =
      "HUAWEICLOUD_TEST_OBS_TEST_RULE_CLASS";
  private static final String HUAWEICLOUD_TEST_OBS_ENDPOINT = "HUAWEICLOUD_TEST_OBS_ENDPOINT";
  private static final String HUAWEICLOUD_TEST_OBS_WAREHOUSE = "HUAWEICLOUD_TEST_OBS_WAREHOUSE";

  private TestUtility() {}

  public static HuaweicloudOBSTestRule initialize() {
    HuaweicloudOBSTestRule testRule;

    String implClass = System.getenv(HUAWEICLOUD_TEST_OBS_RULE_CLASS);
    if (!Strings.isNullOrEmpty(implClass)) {
      LOG.info("The initializing HuaweicloudOBSTestRule implementation is: {}", implClass);
      try {
        DynConstructors.Ctor<HuaweicloudOBSTestRule> ctor =
            DynConstructors.builder(HuaweicloudOBSTestRule.class).impl(implClass).buildChecked();
        testRule = ctor.newInstance();
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot initialize HuaweicloudOBSTestRule, missing no-arg constructor: %s",
                implClass),
            e);
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot initialize HuaweicloudOBSTestRule, %s does not implement it.", implClass),
            e);
      }
    } else {
      LOG.info(
          "Initializing HuaweicloudOBSTestRule implementation with default HuaweicloudOBSMockRule");
      testRule = HuaweicloudOBSMockRule.builder().silent().build();
    }

    return testRule;
  }

  public static String accessKeyId() {
    return System.getenv(HUAWEICLOUD_TEST_ACCESS_KEY_ID);
  }

  public static String accessKeySecret() {
    return System.getenv(HUAWEICLOUD_TEST_ACCESS_KEY_SECRET);
  }

  public static String obsEndpoint() {
    return System.getenv(HUAWEICLOUD_TEST_OBS_ENDPOINT);
  }

  public static String obsWarehouse() {
    return System.getenv(HUAWEICLOUD_TEST_OBS_WAREHOUSE);
  }

  public static String obsBucket() {
    return obsWarehouseURI().bucket();
  }

  public static String obsKey() {
    return obsWarehouseURI().key();
  }

  private static OBSURI obsWarehouseURI() {
    String obsWarehouse = obsWarehouse();
    Preconditions.checkNotNull(
        obsWarehouse,
        "Please set a correct Huaweicloud OBS path for environment variable '%s'",
        HUAWEICLOUD_TEST_OBS_WAREHOUSE);

    return new OBSURI(obsWarehouse);
  }
}
