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
package org.apache.iceberg.tencentcloud.cos;

import com.qcloud.cos.COS;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;
import java.util.HashMap;

public class TestUtility {
  private static final String COS_TEST_ACCESS_KEY_ID = "AKIDcEmNsMrNpJ2n2fE1gUigeUE1iG5lIFA6";
  private static final String COS_TEST_ACCESS_KEY_SECRET = "tcHo9AFNREX7f6ObgdR8f2A97wb9WGBp";
  private static final String COS_TEST_REGION = "ap-beijing";
  private static final String COS_BUCKET = "iceberg-test-1300828920";
  private static final String COS_TMP_DIR = "/tmp";

  private static COS cos;

  private TestUtility() {}

  public static String location(String key) {
    return String.format("cos://%s/%s", cosBucket(), key);
  }

  public static synchronized COS cos() {
    if (cos == null) {
      COSCredentials cred =
          new BasicCOSCredentials(TestUtility.accessKeyId(), TestUtility.accessKeySecret());
      ClientConfig clientConfig = new ClientConfig(new Region(TestUtility.region()));
      cos = new COSClient(cred, clientConfig);
    }
    return cos;
  }

  public static String accessKeyId() {
    return COS_TEST_ACCESS_KEY_ID;
  }

  public static String accessKeySecret() {
    return COS_TEST_ACCESS_KEY_SECRET;
  }

  public static String region() {
    return COS_TEST_REGION;
  }

  public static String cosBucket() {
    return COS_BUCKET;
  }

  public static String cosTmpDir() {
    return COS_TMP_DIR;
  }

  public static CosProperties cosProperties() {
    return new CosProperties(
        new HashMap<String, String>() {
          {
            put(CosProperties.COS_REGION, region());
            put(CosProperties.COS_USERINFO_SECRET_ID, accessKeyId());
            put(CosProperties.COS_USERINFO_SECRET_KEY, accessKeySecret());
            put(CosProperties.COS_TMP_DIR, cosTmpDir());
          }
        });
  }
}
