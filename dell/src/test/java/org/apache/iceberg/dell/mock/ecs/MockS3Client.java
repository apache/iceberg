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

import com.emc.object.Protocol;
import com.emc.object.Range;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.BucketInfo;
import com.emc.object.s3.bean.BucketPolicy;
import com.emc.object.s3.bean.CannedAcl;
import com.emc.object.s3.bean.CompleteMultipartUploadResult;
import com.emc.object.s3.bean.CopyObjectResult;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.CorsConfiguration;
import com.emc.object.s3.bean.DeleteObjectsResult;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.InitiateMultipartUploadResult;
import com.emc.object.s3.bean.LifecycleConfiguration;
import com.emc.object.s3.bean.ListBucketsResult;
import com.emc.object.s3.bean.ListDataNode;
import com.emc.object.s3.bean.ListMultipartUploadsResult;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.ListPartsResult;
import com.emc.object.s3.bean.ListVersionsResult;
import com.emc.object.s3.bean.LocationConstraint;
import com.emc.object.s3.bean.MetadataSearchList;
import com.emc.object.s3.bean.MultipartPartETag;
import com.emc.object.s3.bean.ObjectLockConfiguration;
import com.emc.object.s3.bean.ObjectLockLegalHold;
import com.emc.object.s3.bean.ObjectLockRetention;
import com.emc.object.s3.bean.PingResponse;
import com.emc.object.s3.bean.PutObjectResult;
import com.emc.object.s3.bean.QueryObjectsResult;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.bean.VersioningConfiguration;
import com.emc.object.s3.request.AbortMultipartUploadRequest;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyObjectRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.CreateBucketRequest;
import com.emc.object.s3.request.DeleteObjectRequest;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.GetObjectAclRequest;
import com.emc.object.s3.request.GetObjectLegalHoldRequest;
import com.emc.object.s3.request.GetObjectMetadataRequest;
import com.emc.object.s3.request.GetObjectRequest;
import com.emc.object.s3.request.GetObjectRetentionRequest;
import com.emc.object.s3.request.InitiateMultipartUploadRequest;
import com.emc.object.s3.request.ListBucketsRequest;
import com.emc.object.s3.request.ListMultipartUploadsRequest;
import com.emc.object.s3.request.ListObjectsRequest;
import com.emc.object.s3.request.ListPartsRequest;
import com.emc.object.s3.request.ListVersionsRequest;
import com.emc.object.s3.request.PresignedUrlRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.QueryObjectsRequest;
import com.emc.object.s3.request.SetBucketAclRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import com.emc.object.s3.request.SetObjectLegalHoldRequest;
import com.emc.object.s3.request.SetObjectRetentionRequest;
import com.emc.object.s3.request.UploadPartRequest;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.Assert;

/** Memorized s3 client used in tests. */
public class MockS3Client implements S3Client {

  /**
   * The object data of this client.
   *
   * <p>Current {@link S3ObjectMetadata} only store the user metadata.
   */
  private final Map<ObjectId, ObjectData> objectData = Maps.newConcurrentMap();

  @Override
  public PutObjectResult putObject(PutObjectRequest request) {
    ObjectId objectId = new ObjectId(request.getBucketName(), request.getKey());
    ObjectData data =
        ObjectData.create(convertContent(request.getEntity()), request.getObjectMetadata());
    if (request.getIfMatch() != null) {
      // Compare and swap
      if (this.objectData.computeIfPresent(
              objectId,
              (ignored, oldData) ->
                  oldData.createFullMetadata().getETag().equals(request.getIfMatch())
                      ? data
                      : oldData)
          != data) {
        throw new S3Exception("", 412, "PreconditionFailed", "");
      }
    } else if (request.getIfNoneMatch() != null) {
      // Put if absent
      Assert.assertEquals("If-None-Match only allow *", "*", request.getIfNoneMatch());
      if (this.objectData.putIfAbsent(objectId, data) != null) {
        throw new S3Exception("", 412, "PreconditionFailed", "");
      }
    } else {
      this.objectData.put(objectId, data);
    }
    return new PutObjectResult();
  }

  @Override
  public long appendObject(String bucketName, String key, Object content) {
    ObjectId id = new ObjectId(bucketName, key);
    ObjectData old = objectData.get(id);
    if (old == null) {
      throw new S3Exception("", 404, "NoSuchKey", "");
    }

    byte[] appendedData = convertContent(content);
    if (objectData.replace(id, old, old.appendContent(appendedData))) {
      return old.length();
    } else {
      return wontImplement();
    }
  }

  private byte[] convertContent(Object entity) {
    if (entity instanceof InputStream) {
      try (InputStream inputStream = (InputStream) entity) {
        return ByteStreams.toByteArray(inputStream);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    } else if (entity instanceof byte[]) {
      return ((byte[]) entity).clone();
    }

    throw new IllegalArgumentException(
        String.format("Invalid object entity type %s", entity.getClass()));
  }

  @Override
  public InputStream readObjectStream(String bucketName, String key, Range range) {
    ObjectData data = objectData.get(new ObjectId(bucketName, key));
    if (data == null) {
      throw new S3Exception("", 404, "NoSuchKey", "");
    }

    return data.createInputStream(range);
  }

  @Override
  public S3ObjectMetadata getObjectMetadata(String bucketName, String key) {
    ObjectData data = objectData.get(new ObjectId(bucketName, key));
    if (data == null) {
      throw new S3Exception("", 404, "NoSuchKey", "");
    }

    return data.createFullMetadata();
  }

  @Override
  public void deleteObject(String bucketName, String key) {
    objectData.remove(new ObjectId(bucketName, key));
  }

  @Override
  public GetObjectResult<InputStream> getObject(String bucketName, String key) {
    ObjectData data = objectData.get(new ObjectId(bucketName, key));
    if (data == null) {
      throw new S3Exception("", 404, "NoSuchKey", "");
    }

    GetObjectResult<InputStream> result =
        new GetObjectResult<InputStream>() {
          @Override
          public S3ObjectMetadata getObjectMetadata() {
            return data.createFullMetadata();
          }
        };
    result.setObject(data.createInputStream(Range.fromOffset(0)));
    return result;
  }

  @Override
  public ListObjectsResult listObjects(ListObjectsRequest request) {
    String bucket = request.getBucketName();
    String prefix = request.getPrefix();
    String marker = request.getMarker();
    String delimiter = request.getDelimiter();
    // Use a small default value in mock client
    Assert.assertNull("MaxKeys does not set", request.getMaxKeys());
    int maxKeys = 5;

    List<S3Object> objectResults = Lists.newArrayListWithCapacity(maxKeys);
    Set<String> prefixResults = Sets.newHashSet();
    String nextMarker = null;
    for (Map.Entry<ObjectId, ObjectData> entry :
        objectData.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .collect(Collectors.toList())) {
      ObjectId id = entry.getKey();

      if (!bucket.equals(id.bucket) || !id.name.startsWith(prefix)) {
        continue;
      }

      if (marker != null && id.name.compareTo(marker) < 0) {
        // skip the entry before marker
        continue;
      }

      if (objectResults.size() + prefixResults.size() >= maxKeys) {
        nextMarker = id.name;
        break;
      }

      int nextDelimiter = id.name.indexOf(delimiter, prefix.length());
      if (nextDelimiter > 0) {
        // If name = a/b/c and prefix = a/ , then return a/b/
        prefixResults.add(id.name.substring(0, nextDelimiter + delimiter.length()));
      } else {
        S3Object s3Object = new S3Object();
        s3Object.setKey(id.name);
        s3Object.setETag(entry.getValue().createFullMetadata().getETag());
        objectResults.add(s3Object);
      }
    }

    ListObjectsResult result =
        new ListObjectsResult() {
          @Override
          public List<String> getCommonPrefixes() {
            return prefixResults.stream().sorted().collect(Collectors.toList());
          }
        };
    result.setObjects(objectResults);
    result.setNextMarker(nextMarker);
    return result;
  }

  @Override
  public void destroy() {}

  @Override
  @Deprecated
  public void shutdown() {
    destroy();
  }

  // Following methods won't be implemented. They aren't used in this module.
  @Override
  public ListDataNode listDataNodes() {
    return wontImplement();
  }

  @Override
  public PingResponse pingNode(String host) {
    return wontImplement();
  }

  @Override
  public PingResponse pingNode(Protocol protocol, String host, int port) {
    return wontImplement();
  }

  @Override
  public ListBucketsResult listBuckets() {
    return wontImplement();
  }

  @Override
  public ListBucketsResult listBuckets(ListBucketsRequest request) {
    return wontImplement();
  }

  @Override
  public boolean bucketExists(String bucketName) {
    return wontImplement();
  }

  @Override
  public void createBucket(String bucketName) {
    wontImplement();
  }

  @Override
  public void createBucket(CreateBucketRequest request) {
    wontImplement();
  }

  @Override
  public BucketInfo getBucketInfo(String bucketName) {
    return wontImplement();
  }

  @Override
  public void deleteBucket(String bucketName) {
    wontImplement();
  }

  @Override
  public void setBucketAcl(String bucketName, AccessControlList acl) {
    wontImplement();
  }

  @Override
  public void setBucketAcl(String bucketName, CannedAcl cannedAcl) {
    wontImplement();
  }

  @Override
  public void setBucketAcl(SetBucketAclRequest request) {
    wontImplement();
  }

  @Override
  public AccessControlList getBucketAcl(String bucketName) {
    return wontImplement();
  }

  @Override
  public void setBucketCors(String bucketName, CorsConfiguration corsConfiguration) {
    wontImplement();
  }

  @Override
  public CorsConfiguration getBucketCors(String bucketName) {
    return wontImplement();
  }

  @Override
  public void deleteBucketCors(String bucketName) {
    wontImplement();
  }

  @Override
  public void setBucketLifecycle(String bucketName, LifecycleConfiguration lifecycleConfiguration) {
    wontImplement();
  }

  @Override
  public LifecycleConfiguration getBucketLifecycle(String bucketName) {
    return wontImplement();
  }

  @Override
  public void deleteBucketLifecycle(String bucketName) {
    wontImplement();
  }

  @Override
  public void setBucketPolicy(String bucketName, BucketPolicy policy) {
    wontImplement();
  }

  @Override
  public BucketPolicy getBucketPolicy(String bucketName) {
    return wontImplement();
  }

  @Override
  public LocationConstraint getBucketLocation(String bucketName) {
    return wontImplement();
  }

  @Override
  public void setBucketVersioning(
      String bucketName, VersioningConfiguration versioningConfiguration) {
    wontImplement();
  }

  @Override
  public VersioningConfiguration getBucketVersioning(String bucketName) {
    return wontImplement();
  }

  @Override
  public void setBucketStaleReadAllowed(String bucketName, boolean staleReadsAllowed) {
    wontImplement();
  }

  @Override
  public MetadataSearchList listSystemMetadataSearchKeys() {
    return wontImplement();
  }

  @Override
  public MetadataSearchList listBucketMetadataSearchKeys(String bucketName) {
    return wontImplement();
  }

  @Override
  public QueryObjectsResult queryObjects(QueryObjectsRequest request) {
    return wontImplement();
  }

  @Override
  public QueryObjectsResult queryMoreObjects(QueryObjectsResult lastResult) {
    return wontImplement();
  }

  @Override
  public ListObjectsResult listObjects(String bucketName) {
    return wontImplement();
  }

  @Override
  public ListObjectsResult listObjects(String bucketName, String prefix) {
    return wontImplement();
  }

  @Override
  public ListObjectsResult listMoreObjects(ListObjectsResult lastResult) {
    return wontImplement();
  }

  @Override
  public ListVersionsResult listVersions(String bucketName, String prefix) {
    return wontImplement();
  }

  @Override
  public ListVersionsResult listVersions(ListVersionsRequest request) {
    return wontImplement();
  }

  @Override
  public ListVersionsResult listMoreVersions(ListVersionsResult lastResult) {
    return wontImplement();
  }

  @Override
  public void putObject(String bucketName, String key, Object content, String contentType) {
    wontImplement();
  }

  @Override
  public void putObject(String bucketName, String key, Range range, Object content) {
    wontImplement();
  }

  @Override
  public CopyObjectResult copyObject(
      String sourceBucketName, String sourceKey, String bucketName, String key) {
    return wontImplement();
  }

  @Override
  public CopyObjectResult copyObject(CopyObjectRequest request) {
    return wontImplement();
  }

  @Override
  public <T> T readObject(String bucketName, String key, Class<T> objectType) {
    return wontImplement();
  }

  @Override
  public <T> T readObject(String bucketName, String key, String versionId, Class<T> objectType) {
    return wontImplement();
  }

  @Override
  public <T> GetObjectResult<T> getObject(GetObjectRequest request, Class<T> objectType) {
    return wontImplement();
  }

  @Override
  public URL getPresignedUrl(String bucketName, String key, Date expirationTime) {
    return wontImplement();
  }

  @Override
  public URL getPresignedUrl(PresignedUrlRequest request) {
    return wontImplement();
  }

  @Override
  public void deleteObject(DeleteObjectRequest request) {
    wontImplement();
  }

  @Override
  public void deleteVersion(String bucketName, String key, String versionId) {
    wontImplement();
  }

  @Override
  public DeleteObjectsResult deleteObjects(DeleteObjectsRequest request) {
    return wontImplement();
  }

  @Override
  public void setObjectMetadata(String bucketName, String key, S3ObjectMetadata objectMetadata) {
    wontImplement();
  }

  @Override
  public S3ObjectMetadata getObjectMetadata(GetObjectMetadataRequest request) {
    return wontImplement();
  }

  @Override
  public void setObjectAcl(String bucketName, String key, AccessControlList acl) {
    wontImplement();
  }

  @Override
  public void setObjectAcl(String bucketName, String key, CannedAcl cannedAcl) {
    wontImplement();
  }

  @Override
  public void setObjectAcl(SetObjectAclRequest request) {
    wontImplement();
  }

  @Override
  public AccessControlList getObjectAcl(String bucketName, String key) {
    return wontImplement();
  }

  @Override
  public AccessControlList getObjectAcl(GetObjectAclRequest request) {
    return wontImplement();
  }

  @Override
  public void extendRetentionPeriod(String bucketName, String key, Long period) {
    wontImplement();
  }

  @Override
  public ListMultipartUploadsResult listMultipartUploads(String bucketName) {
    return wontImplement();
  }

  @Override
  public ListMultipartUploadsResult listMultipartUploads(ListMultipartUploadsRequest request) {
    return wontImplement();
  }

  @Override
  public String initiateMultipartUpload(String bucketName, String key) {
    return wontImplement();
  }

  @Override
  public InitiateMultipartUploadResult initiateMultipartUpload(
      InitiateMultipartUploadRequest request) {
    return wontImplement();
  }

  @Override
  public ListPartsResult listParts(String bucketName, String key, String uploadId) {
    return wontImplement();
  }

  @Override
  public ListPartsResult listParts(ListPartsRequest request) {
    return wontImplement();
  }

  @Override
  public MultipartPartETag uploadPart(UploadPartRequest request) {
    return wontImplement();
  }

  @Override
  public CopyPartResult copyPart(CopyPartRequest request) {
    return wontImplement();
  }

  @Override
  public CompleteMultipartUploadResult completeMultipartUpload(
      CompleteMultipartUploadRequest request) {
    return wontImplement();
  }

  @Override
  public void abortMultipartUpload(AbortMultipartUploadRequest request) {
    wontImplement();
  }

  @Override
  public void setObjectLockConfiguration(
      String bucketName, ObjectLockConfiguration objectLockConfiguration) {
    wontImplement();
  }

  @Override
  public ObjectLockConfiguration getObjectLockConfiguration(String bucketName) {
    return wontImplement();
  }

  @Override
  public void enableObjectLock(String bucketName) {
    wontImplement();
  }

  @Override
  public void setObjectLegalHold(SetObjectLegalHoldRequest request) {
    wontImplement();
  }

  @Override
  public ObjectLockLegalHold getObjectLegalHold(GetObjectLegalHoldRequest request) {
    return wontImplement();
  }

  @Override
  public void setObjectRetention(SetObjectRetentionRequest request) {
    wontImplement();
  }

  @Override
  public ObjectLockRetention getObjectRetention(GetObjectRetentionRequest request) {
    return wontImplement();
  }

  private <T> T wontImplement() {
    throw new UnsupportedOperationException();
  }
}
