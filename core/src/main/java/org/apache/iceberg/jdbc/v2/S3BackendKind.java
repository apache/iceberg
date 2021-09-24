/*
 *    Copyright 2021 Two Sigma Open Source, LLC
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.apache.iceberg.jdbc.v2;

import com.amazonaws.SdkClientException;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

// TODO I assume there's common module in Iceberg that deals with S3, which should replace this class.
// This class only exists as utils so that S3 backends in our environment work properly.
public enum S3BackendKind {
    GCS("gs") {
        @Override
        public AmazonS3 createS3Client(String projectId) {
            Preconditions.checkArgument(
                    !Strings.isNullOrEmpty(projectId),
                    "Must provide a projectId when creating gcs client.");
            // specific credential provider and request handlers ignored
            return AmazonS3ClientBuilder.standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("https://storage.googleapis.com", "auto"))
                    .build();
        }
    };

    public final String scheme;

    S3BackendKind(String scheme) {
        this.scheme = scheme;
    }

    public abstract AmazonS3 createS3Client(String projectId);

    public static S3BackendKind fromName(String name) {
        for (final S3BackendKind type : S3BackendKind.values()) {
            if (type.name().equalsIgnoreCase(name)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported backend: " + name);
    }

    public static S3BackendKind fromScheme(String scheme) {
        for (final S3BackendKind type : S3BackendKind.values()) {
            if (type.scheme.equals(scheme)) {
                return type;
            }
        }
        throw new IllegalArgumentException("No S3 impl backing the scheme: " + scheme);
    }

    public static List<S3ObjectSummary> globAllObjects(AmazonS3 s3, String bucket, String prefix) {
        final ListObjectsV2Request request = new ListObjectsV2Request();
        request.setMaxKeys(1000);
        request.setBucketName(bucket);
        request.setPrefix(prefix);

        final ImmutableList.Builder<S3ObjectSummary> builder = ImmutableList.builder();
        while (true) {
            final ListObjectsV2Result result = s3.listObjectsV2(request);

            builder.addAll(result.getObjectSummaries());
            if (!result.isTruncated()) {
                break;
            }
            request.setContinuationToken(result.getNextContinuationToken());
        }

        return builder.build();
    }

    public static List<String> deleteAllKeysDeleteObject(
            AmazonS3 s3, String bucket, List<String> keys) {
        final ImmutableList.Builder<String> deleted = ImmutableList.builder();
        for (final String key : keys) {
            try {
                s3.deleteObject(bucket, key);
                deleted.add(key);
            } catch (SdkClientException e) {
                continue;
            }
        }
        return deleted.build();
    }
}
