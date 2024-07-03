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

import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.HttpStatus.PARTIAL_CONTENT;
import static org.springframework.http.HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE;

import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.model.Bucket;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@RestController
public class AliyunOSSMockLocalController {
  private static final Logger LOG = LoggerFactory.getLogger(AliyunOSSMockLocalController.class);

  @Autowired private AliyunOSSMockLocalStore localStore;

  private static String filenameFrom(@PathVariable String bucketName, HttpServletRequest request) {
    String requestUri = request.getRequestURI();
    return requestUri.substring(requestUri.indexOf(bucketName) + bucketName.length() + 1);
  }

  @RequestMapping(value = "/{bucketName}", method = RequestMethod.PUT, produces = "application/xml")
  public void putBucket(@PathVariable String bucketName) throws IOException {
    if (localStore.getBucket(bucketName) != null) {
      throw new OssException(
          409, OSSErrorCode.BUCKET_ALREADY_EXISTS, bucketName + " already exists.");
    }

    localStore.createBucket(bucketName);
  }

  @RequestMapping(
      value = "/{bucketName}",
      method = RequestMethod.DELETE,
      produces = "application/xml")
  public void deleteBucket(@PathVariable String bucketName) throws IOException {
    verifyBucketExistence(bucketName);

    localStore.deleteBucket(bucketName);
  }

  @RequestMapping(value = "/{bucketName:.+}/**", method = RequestMethod.PUT)
  public ResponseEntity<String> putObject(
      @PathVariable String bucketName, HttpServletRequest request) {
    verifyBucketExistence(bucketName);
    String filename = filenameFrom(bucketName, request);
    try (ServletInputStream inputStream = request.getInputStream()) {
      ObjectMetadata metadata =
          localStore.putObject(
              bucketName,
              filename,
              inputStream,
              request.getContentType(),
              request.getHeader(HttpHeaders.CONTENT_ENCODING),
              ImmutableMap.of());

      HttpHeaders responseHeaders = new HttpHeaders();
      responseHeaders.setETag("\"" + metadata.getContentMD5() + "\"");
      responseHeaders.setLastModified(metadata.getLastModificationDate());

      return new ResponseEntity<>(responseHeaders, OK);
    } catch (Exception e) {
      LOG.error("Failed to put object - bucket: {} - object: {}", bucketName, filename, e);
      return new ResponseEntity<>(e.getMessage(), INTERNAL_SERVER_ERROR);
    }
  }

  @RequestMapping(value = "/{bucketName:.+}/**", method = RequestMethod.DELETE)
  public void deleteObject(@PathVariable String bucketName, HttpServletRequest request) {
    verifyBucketExistence(bucketName);

    localStore.deleteObject(bucketName, filenameFrom(bucketName, request));
  }

  @RequestMapping(value = "/{bucketName:.+}/**", method = RequestMethod.HEAD)
  public ResponseEntity<String> getObjectMeta(
      @PathVariable String bucketName, HttpServletRequest request) {
    verifyBucketExistence(bucketName);
    ObjectMetadata metadata = verifyObjectExistence(bucketName, filenameFrom(bucketName, request));

    HttpHeaders headers = new HttpHeaders();
    headers.setETag("\"" + metadata.getContentMD5() + "\"");
    headers.setLastModified(metadata.getLastModificationDate());
    headers.setContentLength(metadata.getContentLength());

    return new ResponseEntity<>(headers, OK);
  }

  @SuppressWarnings("checkstyle:AnnotationUseStyle")
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      method = RequestMethod.GET,
      produces = "application/xml")
  public void getObject(
      @PathVariable String bucketName,
      @RequestHeader(value = "Range", required = false) Range range,
      HttpServletRequest request,
      HttpServletResponse response)
      throws IOException {
    verifyBucketExistence(bucketName);

    String filename = filenameFrom(bucketName, request);
    ObjectMetadata metadata = verifyObjectExistence(bucketName, filename);

    if (range != null) {
      long fileSize = metadata.getContentLength();
      long bytesToRead = Math.min(fileSize - 1, range.end()) - range.start() + 1;
      long skipSize = range.start();
      if (range.start() == -1) {
        bytesToRead = Math.min(fileSize - 1, range.end());
        skipSize = fileSize - range.end();
      }
      if (range.end() == -1) {
        bytesToRead = fileSize - range.start();
      }
      if (bytesToRead < 0 || fileSize < range.start()) {
        response.setStatus(REQUESTED_RANGE_NOT_SATISFIABLE.value());
        response.flushBuffer();
        return;
      }

      response.setStatus(PARTIAL_CONTENT.value());
      response.setHeader(HttpHeaders.ACCEPT_RANGES, "bytes");
      response.setHeader(
          HttpHeaders.CONTENT_RANGE,
          String.format(
              "bytes %s-%s/%s",
              range.start(), bytesToRead + range.start() + 1, metadata.getContentLength()));
      response.setHeader(HttpHeaders.ETAG, "\"" + metadata.getContentMD5() + "\"");
      response.setDateHeader(HttpHeaders.LAST_MODIFIED, metadata.getLastModificationDate());
      response.setContentType(metadata.getContentType());
      response.setContentLengthLong(bytesToRead);

      try (OutputStream outputStream = response.getOutputStream()) {
        try (FileInputStream fis = new FileInputStream(metadata.getDataFile())) {
          fis.skip(skipSize);
          ByteStreams.copy(new BoundedInputStream(fis, bytesToRead), outputStream);
        }
      }
    } else {
      response.setHeader(HttpHeaders.ACCEPT_RANGES, "bytes");
      response.setHeader(HttpHeaders.ETAG, "\"" + metadata.getContentMD5() + "\"");
      response.setDateHeader(HttpHeaders.LAST_MODIFIED, metadata.getLastModificationDate());
      response.setContentType(metadata.getContentType());
      response.setContentLengthLong(metadata.getContentLength());

      try (OutputStream outputStream = response.getOutputStream()) {
        try (FileInputStream fis = new FileInputStream(metadata.getDataFile())) {
          ByteStreams.copy(fis, outputStream);
        }
      }
    }
  }

  private void verifyBucketExistence(String bucketName) {
    Bucket bucket = localStore.getBucket(bucketName);
    if (bucket == null) {
      throw new OssException(
          404, OSSErrorCode.NO_SUCH_BUCKET, "The specified bucket does not exist. ");
    }
  }

  private ObjectMetadata verifyObjectExistence(String bucketName, String filename) {
    ObjectMetadata objectMetadata = null;
    try {
      objectMetadata = localStore.getObjectMetadata(bucketName, filename);
    } catch (IOException e) {
      LOG.error(
          "Failed to get the object metadata, bucket: {}, object: {}.", bucketName, filename, e);
    }

    if (objectMetadata == null) {
      throw new OssException(404, OSSErrorCode.NO_SUCH_KEY, "The specify oss key does not exists.");
    }

    return objectMetadata;
  }

  @ControllerAdvice
  public static class OSSMockExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler
    public ResponseEntity<ErrorResponse> handleOSSException(OssException ex) {
      LOG.info("Responding with status {} - {}, {}", ex.status, ex.code, ex.message);

      ErrorResponse errorResponse = new ErrorResponse();
      errorResponse.setCode(ex.getCode());
      errorResponse.setMessage(ex.getMessage());

      HttpHeaders headers = new HttpHeaders();
      headers.setContentType(MediaType.APPLICATION_XML);

      return ResponseEntity.status(ex.status).headers(headers).body(errorResponse);
    }
  }

  public static class OssException extends RuntimeException {

    private final int status;
    private final String code;
    private final String message;

    public OssException(final int status, final String code, final String message) {
      super(message);
      this.status = status;
      this.code = code;
      this.message = message;
    }

    public String getCode() {
      return code;
    }

    @Override
    public String getMessage() {
      return message;
    }
  }

  @JsonRootName("Error")
  public static class ErrorResponse {
    @JsonProperty("Code")
    private String code;

    @JsonProperty("Message")
    private String message;

    public void setCode(String code) {
      this.code = code;
    }

    public void setMessage(String message) {
      this.message = message;
    }
  }

  /**
   * Reads bytes up to a maximum length, if its count goes above that, it stops.
   *
   * <p>This is useful to wrap ServletInputStreams. The ServletInputStream will block if you try to
   * read content from it that isn't there, because it doesn't know whether the content hasn't
   * arrived yet or whether the content has finished. So, one of these, initialized with the
   * Content-length sent in the ServletInputStream's header, will stop it blocking, providing it's
   * been sent with a correct content length.
   *
   * <p>This code is borrowed from `org.apache.commons:commons-io`
   */
  public class BoundedInputStream extends FilterInputStream {

    /** The max count of bytes to read. */
    private final long maxCount;

    /** The count of bytes read. */
    private long count;

    /** The marked position. */
    private long mark = -1;

    /** Flag if close should be propagated. */
    private boolean propagateClose = true;

    /**
     * Constructs a new {@link BoundedInputStream} that wraps the given input stream and is
     * unlimited.
     *
     * @param in The wrapped input stream.
     */
    public BoundedInputStream(final InputStream in) {
      this(in, -1);
    }

    /**
     * Constructs a new {@link BoundedInputStream} that wraps the given input stream and limits it
     * to a certain size.
     *
     * @param inputStream The wrapped input stream.
     * @param maxLength The maximum number of bytes to return.
     */
    public BoundedInputStream(final InputStream inputStream, final long maxLength) {
      // Some badly designed methods - e.g. the servlet API - overload length
      // such that "-1" means stream finished
      super(inputStream);
      this.maxCount = maxLength;
    }

    /** {@inheritDoc} */
    @Override
    public int available() throws IOException {
      if (isMaxLength()) {
        onMaxLength(maxCount, count);
        return 0;
      }
      return in.available();
    }

    /**
     * Invokes the delegate's {@code close()} method if {@link #isPropagateClose()} is {@code true}.
     *
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
      if (propagateClose) {
        in.close();
      }
    }

    /**
     * Gets the count of bytes read.
     *
     * @return The count of bytes read.
     * @since 2.12.0
     */
    public long getCount() {
      return count;
    }

    /**
     * Gets the max count of bytes to read.
     *
     * @return The max count of bytes to read.
     * @since 2.12.0
     */
    public long getMaxLength() {
      return maxCount;
    }

    private boolean isMaxLength() {
      return maxCount >= 0 && count >= maxCount;
    }

    /**
     * Tests whether the {@link #close()} method should propagate to the underling {@link
     * InputStream}.
     *
     * @return {@code true} if calling {@link #close()} propagates to the {@code close()} method of
     *     the underlying stream or {@code false} if it does not.
     */
    public boolean isPropagateClose() {
      return propagateClose;
    }

    /**
     * Sets whether the {@link #close()} method should propagate to the underling {@link
     * InputStream}.
     *
     * @param propagateClose {@code true} if calling {@link #close()} propagates to the {@code
     *     close()} method of the underlying stream or {@code false} if it does not.
     */
    public void setPropagateClose(final boolean propagateClose) {
      this.propagateClose = propagateClose;
    }

    /**
     * Invokes the delegate's {@code mark(int)} method.
     *
     * @param readlimit read ahead limit
     */
    @Override
    public synchronized void mark(final int readlimit) {
      in.mark(readlimit);
      mark = count;
    }

    /**
     * Invokes the delegate's {@code markSupported()} method.
     *
     * @return true if mark is supported, otherwise false
     */
    @Override
    public boolean markSupported() {
      return in.markSupported();
    }

    /**
     * A caller has caused a request that would cross the {@code maxLength} boundary.
     *
     * @param maxLength The max count of bytes to read.
     * @param bytesRead The count of bytes read.
     * @throws IOException Subclasses may throw.
     * @since 2.12.0
     */
    protected void onMaxLength(final long maxLength, final long bytesRead) throws IOException {
      // for subclasses
    }

    /**
     * Invokes the delegate's {@code read()} method if the current position is less than the limit.
     *
     * @return the byte read or -1 if the end of stream or the limit has been reached.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public int read() throws IOException {
      if (isMaxLength()) {
        onMaxLength(maxCount, count);
        return -1;
      }
      final int result = in.read();
      count++;
      return result;
    }

    /**
     * Invokes the delegate's {@code read(byte[])} method.
     *
     * @param b the buffer to read the bytes into
     * @return the number of bytes read or -1 if the end of stream or the limit has been reached.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public int read(final byte[] b) throws IOException {
      return this.read(b, 0, b.length);
    }

    /**
     * Invokes the delegate's {@code read(byte[], int, int)} method.
     *
     * @param b the buffer to read the bytes into
     * @param off The start offset
     * @param len The number of bytes to read
     * @return the number of bytes read or -1 if the end of stream or the limit has been reached.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      if (isMaxLength()) {
        onMaxLength(maxCount, count);
        return -1;
      }
      final long maxRead = maxCount >= 0 ? Math.min(len, maxCount - count) : len;
      final int bytesRead = in.read(b, off, (int) maxRead);

      if (bytesRead == -1) {
        return -1;
      }

      count += bytesRead;
      return bytesRead;
    }

    /**
     * Invokes the delegate's {@code reset()} method.
     *
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public synchronized void reset() throws IOException {
      in.reset();
      count = mark;
    }

    /**
     * Invokes the delegate's {@code skip(long)} method.
     *
     * @param n the number of bytes to skip
     * @return the actual number of bytes skipped
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public long skip(final long n) throws IOException {
      final long toSkip = maxCount >= 0 ? Math.min(n, maxCount - count) : n;
      final long skippedBytes = in.skip(toSkip);
      count += skippedBytes;
      return skippedBytes;
    }

    /**
     * Invokes the delegate's {@code toString()} method.
     *
     * @return the delegate's {@code toString()}
     */
    @Override
    public String toString() {
      return in.toString();
    }
  }
}
