/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.aws;

import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.utils.ToString;
import software.amazon.awssdk.utils.Validate;
import java.util.Map;

/**
 * An implementation of {@link AwsCredentialsProvider} that returns a set implementation of {@link AwsCredentials}.
 */
@SdkPublicApi
public final class StaticCredentialsProvider implements AwsCredentialsProvider {
    private static final String PROVIDER_NAME = "StaticCredentialsProvider";
    private static final String ACCESS_KEY_ID = "access-key-id";
    private static final String SECRET_ACCESS_KEY = "secret-access-key";
    private final AwsCredentials credentials;

    private StaticCredentialsProvider(AwsCredentials credentials) {
        Validate.notNull(credentials, "Credentials must not be null.");
        this.credentials = withProviderName(credentials);
    }

    private AwsCredentials withProviderName(AwsCredentials credentials) {
        if (credentials instanceof AwsBasicCredentials) {
            return ((AwsBasicCredentials) credentials).copy(c -> c.providerName(PROVIDER_NAME));
        }
        if (credentials instanceof AwsSessionCredentials) {
            return ((AwsSessionCredentials) credentials).copy(c -> c.providerName(PROVIDER_NAME));
        }
        return credentials;
    }

    /**
     * Create a credentials provider that always returns the provided set of credentials.
     */
    public static StaticCredentialsProvider create(Map<String, String> credentials) {
        return new StaticCredentialsProvider(AwsBasicCredentials.create(credentials.get(ACCESS_KEY_ID), credentials.get(SECRET_ACCESS_KEY)));
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return credentials;
    }

    @Override
    public String toString() {
        return ToString.builder(PROVIDER_NAME)
                .add("credentials", credentials)
                .build();
    }
}
