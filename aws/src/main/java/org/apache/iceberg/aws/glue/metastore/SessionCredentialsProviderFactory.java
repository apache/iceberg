/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.aws.glue.metastore;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class SessionCredentialsProviderFactory implements AWSCredentialsProviderFactory {

  public static final String AWS_ACCESS_KEY_CONF_VAR = "hive.aws_session_access_id";
  public static final String AWS_SECRET_KEY_CONF_VAR = "hive.aws_session_secret_key";
  public static final String AWS_SESSION_TOKEN_CONF_VAR = "hive.aws_session_token";

  @Override
  public AWSCredentialsProvider buildAWSCredentialsProvider(HiveConf hiveConf) {

    Preconditions.checkArgument(hiveConf != null, "hiveConf cannot be null.");

    String accessKey = hiveConf.get(AWS_ACCESS_KEY_CONF_VAR);
    String secretKey = hiveConf.get(AWS_SECRET_KEY_CONF_VAR);
    String sessionToken = hiveConf.get(AWS_SESSION_TOKEN_CONF_VAR);

    Preconditions.checkArgument(accessKey != null, AWS_ACCESS_KEY_CONF_VAR + " must be set.");
    Preconditions.checkArgument(secretKey != null, AWS_SECRET_KEY_CONF_VAR + " must be set.");
    Preconditions.checkArgument(sessionToken != null, AWS_SESSION_TOKEN_CONF_VAR + " must be set.");

    AWSSessionCredentials credentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken);

    return new StaticCredentialsProvider(credentials);
  }
}
