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

import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.model.Bucket;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class AliyunOSSMock {

  static final String PROP_ROOT_DIR = "root-dir";
  static final String ROOT_DIR_DEFAULT = "/tmp";

  static final String PROP_HTTP_PORT = "server.port";
  static final int PORT_HTTP_PORT_DEFAULT = 9393;

  // SET to 2 to test list prefix with paging
  static final int LIST_OBJECT_MAX_KEYS = 2;

  static final String SPECIAL_KEY_TRIGGER_MOCK_SERVER_ERROR = "mock-internal-error-key";

  private final AliyunOSSMockLocalStore localStore;
  private final HttpServer httpServer;

  public static AliyunOSSMock start(Map<String, Object> properties) throws IOException {
    AliyunOSSMock mock =
        new AliyunOSSMock(
            properties.getOrDefault(PROP_ROOT_DIR, ROOT_DIR_DEFAULT).toString(),
            Integer.parseInt(
                properties.getOrDefault(PROP_HTTP_PORT, PORT_HTTP_PORT_DEFAULT).toString()));
    mock.start();
    return mock;
  }

  private AliyunOSSMock(String rootDir, int serverPort) throws IOException {
    localStore = new AliyunOSSMockLocalStore(rootDir);
    httpServer = HttpServer.create(new InetSocketAddress("localhost", serverPort), 0);
  }

  private void start() {
    httpServer.createContext("/", new AliyunHttpHandler());
    httpServer.start();
  }

  public void stop() {
    httpServer.stop(0);
  }

  private static final Map<String, List<ObjectMetadata>> LIST_PREFIX_PAGES = Maps.newHashMap();

  private class AliyunHttpHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
      String request = httpExchange.getRequestURI().getPath().substring(1);
      String[] requests = request.split("/");
      String bucketName = requests[0];
      if (requests.length == 1) {
        // bucket operations
        if (httpExchange.getRequestMethod().equals("PUT")) {
          putBucket(bucketName, httpExchange);
        }
        if (httpExchange.getRequestMethod().equals("DELETE")) {
          deleteBucket(bucketName, httpExchange);
        }
        if (httpExchange.getRequestMethod().equals("GET")) {
          listObjects(bucketName, httpExchange);
        }
        if (httpExchange.getRequestMethod().equals("POST")) {
          deleteObjects(bucketName, httpExchange);
        }
      } else {
        // object operations
        String objectName = request.substring(bucketName.length() + 1);
        if (objectName.contains("?")) {
          objectName = objectName.substring(0, objectName.indexOf("?"));
        }
        if (httpExchange.getRequestMethod().equals("PUT")) {
          putObject(bucketName, objectName, httpExchange);
        }
        if (httpExchange.getRequestMethod().equals("DELETE")) {
          deleteObject(bucketName, objectName, httpExchange);
        }
        if (httpExchange.getRequestMethod().equals("HEAD")) {
          getObjectMeta(bucketName, objectName, httpExchange);
        }
        if (httpExchange.getRequestMethod().equals("GET")) {
          getObject(bucketName, objectName, httpExchange);
        }
      }
    }

    private void putBucket(String bucketName, HttpExchange httpExchange) throws IOException {
      if (localStore.getBucket(bucketName) != null) {
        String errorMessage =
            createErrorResponse(
                OSSErrorCode.BUCKET_ALREADY_EXISTS, bucketName + " already exists.");
        handleResponse(httpExchange, 409, errorMessage, "application/xml");
        return;
      }
      localStore.createBucket(bucketName);
      handleResponse(httpExchange, 200, "OK", "application/xml");
    }

    private void deleteBucket(String bucketName, HttpExchange httpExchange) throws IOException {
      boolean bucketExist = verifyBucketExistence(bucketName, httpExchange);
      if (!bucketExist) {
        return;
      }
      try {
        localStore.deleteBucket(bucketName);
      } catch (Exception e) {
        String errorMessage =
            createErrorResponse(
                OSSErrorCode.BUCKET_NOT_EMPTY, "The bucket you tried to delete is not empty.");
        handleResponse(httpExchange, 409, errorMessage, "application/xml");
      }
      handleResponse(httpExchange, 200, "OK", "application/xml");
    }

    private void putObject(String bucketName, String objectName, HttpExchange httpExchange)
        throws IOException {
      boolean bucketExist = verifyBucketExistence(bucketName, httpExchange);
      if (!bucketExist) {
        return;
      }
      try (InputStream inputStream = httpExchange.getRequestBody()) {
        ObjectMetadata metadata =
            localStore.putObject(
                bucketName,
                objectName,
                inputStream,
                httpExchange.getRequestHeaders().getFirst("Content-Type"),
                httpExchange.getRequestHeaders().getFirst("Content-Headers"),
                ImmutableMap.of());

        httpExchange.getResponseHeaders().add("ETag", metadata.getContentMD5());
        httpExchange
            .getResponseHeaders()
            .add(
                "Last-Modified",
                createDate(
                    metadata.getLastModificationDate(), DateTimeFormatter.RFC_1123_DATE_TIME));
        handleResponse(httpExchange, 200, "OK", "text/plain");
      } catch (Exception e) {
        handleResponse(httpExchange, 500, "Internal Server Error", "text/plain");
      }
    }

    private void deleteObject(String bucketName, String objectName, HttpExchange httpExchange)
        throws IOException {
      boolean bucketExist = verifyBucketExistence(bucketName, httpExchange);
      if (!bucketExist) {
        return;
      }
      localStore.deleteObject(bucketName, objectName);

      handleResponse(httpExchange, 200, "OK", "text/plain");
    }

    private void getObjectMeta(String bucketName, String objectName, HttpExchange httpExchange)
        throws IOException {
      boolean bucketExist = verifyBucketExistence(bucketName, httpExchange);
      if (!bucketExist) {
        return;
      }
      ObjectMetadata metadata = verifyObjectExistence(bucketName, objectName);

      if (metadata == null) {
        String errorMessage =
            createErrorResponse(OSSErrorCode.NO_SUCH_KEY, "The specify oss key does not exists.");
        handleResponse(httpExchange, 404, errorMessage, "application/xml");
      } else {
        httpExchange.getResponseHeaders().add("ETag", metadata.getContentMD5());
        httpExchange
            .getResponseHeaders()
            .add(
                "Last-Modified",
                createDate(
                    metadata.getLastModificationDate(), DateTimeFormatter.RFC_1123_DATE_TIME));
        httpExchange
            .getResponseHeaders()
            .add("Content-Length", Long.toString(metadata.getContentLength()));

        handleResponse(httpExchange, 200, "OK", "text/plain");
      }
    }

    private void getObject(String bucketName, String objectName, HttpExchange httpExchange)
        throws IOException {
      verifyBucketExistence(bucketName, httpExchange);

      String filename = objectName;
      ObjectMetadata metadata = verifyObjectExistence(bucketName, filename);

      if (metadata == null) {
        String errorMessage =
            createErrorResponse(OSSErrorCode.NO_SUCH_KEY, "The specify oss key does not exists.");
        handleResponse(httpExchange, 404, errorMessage, "application/xml");
        return;
      }

      Object range = httpExchange.getRequestHeaders().get("Range");
      if (range != null) {
        range = range.toString().replace("[bytes=", "").replace("]", "");
        String[] ranges = range.toString().split("-");
        long rangeStart = -1;
        if (!ranges[0].isEmpty()) {
          rangeStart = Long.parseLong(ranges[0]);
        }
        long rangeEnd = -1;
        if (ranges.length == 2 && !ranges[1].isEmpty()) {
          rangeEnd = Long.parseLong(ranges[1]);
        }
        if (rangeEnd == -1) {
          rangeEnd = Long.MAX_VALUE;
          if (rangeStart == -1) {
            rangeStart = 0;
          }
        }

        long fileSize = metadata.getContentLength();
        long bytesToRead = Math.min(fileSize - 1, rangeEnd) - rangeStart + 1;
        long skipSize = rangeStart;
        if (rangeStart == -1) {
          bytesToRead = Math.min(fileSize - 1, rangeEnd);
          skipSize = fileSize - rangeEnd;
        }
        if (rangeEnd == -1) {
          bytesToRead = fileSize - rangeStart;
        }
        if (bytesToRead < 0 || fileSize < rangeStart) {
          httpExchange.sendResponseHeaders(416, 1);
          return;
        }

        httpExchange.getResponseHeaders().add("Accept-Ranges", "bytes");
        httpExchange
            .getResponseHeaders()
            .add(
                "Content-Range",
                "bytes "
                    + rangeStart
                    + "-"
                    + (bytesToRead + rangeStart + 1)
                    + "/"
                    + metadata.getContentLength());
        httpExchange.getResponseHeaders().add("ETag", metadata.getContentMD5());
        httpExchange
            .getResponseHeaders()
            .add(
                "Last-Modified",
                createDate(
                    metadata.getLastModificationDate(), DateTimeFormatter.RFC_1123_DATE_TIME));
        httpExchange.getResponseHeaders().add("Content-Type", metadata.getContentType());
        httpExchange.getResponseHeaders().add("Content-Length", Long.toString(bytesToRead));
        httpExchange.sendResponseHeaders(206, bytesToRead);
        try (OutputStream outputStream = httpExchange.getResponseBody()) {
          try (FileInputStream fis = new FileInputStream(metadata.getDataFile())) {
            fis.skip(skipSize);
            ByteStreams.copy(new BoundedInputStream(fis, bytesToRead), outputStream);
          }
        }
      } else {
        httpExchange.getResponseHeaders().add("Accept-Ranges", "bytes");
        httpExchange.getResponseHeaders().add("ETag", metadata.getContentMD5());
        httpExchange
            .getResponseHeaders()
            .add(
                "Last-Modified",
                createDate(
                    metadata.getLastModificationDate(), DateTimeFormatter.RFC_1123_DATE_TIME));
        httpExchange.getResponseHeaders().add("Content-Type", metadata.getContentType());
        httpExchange.sendResponseHeaders(200, metadata.getContentLength());

        try (OutputStream outputStream = httpExchange.getResponseBody()) {
          try (FileInputStream fis = new FileInputStream(metadata.getDataFile())) {
            ByteStreams.copy(fis, outputStream);
          }
        }
      }
    }

    private void listObjects(String bucketName, HttpExchange httpExchange) throws IOException {
      boolean bucketExist = verifyBucketExistence(bucketName, httpExchange);
      if (!bucketExist) {
        return;
      }
      // Parse query parameters for ListObjectsV2
      String query = httpExchange.getRequestURI().getQuery();
      String prefix = null;
      String listType = null;
      String continuationToken = null;
      if (query != null) {
        String[] params = query.split("&");
        for (String param : params) {
          if (param.startsWith("prefix=")) {
            prefix = param.substring("prefix=".length());
            try {
              prefix = java.net.URLDecoder.decode(prefix, "UTF-8");
            } catch (Exception e) {
              handleResponse(httpExchange, 400, "Bad Request - Cannot decode prefix", "text/plain");
              return;
            }
          } else if (param.startsWith("list-type=")) {
            listType = param.substring("list-type=".length());
          } else if (param.startsWith("continuation-token=")) {
            continuationToken = param.substring("continuation-token=".length());
          }
        }
      }

      // Only handle ListObjectsV2 requests
      if (!"2".equals(listType)) {
        handleResponse(
            httpExchange, 400, "Bad Request - Only ListObjectsV2 supported", "text/plain");
        return;
      }
      if (continuationToken == null) {
        continuationToken = UUID.randomUUID().toString();
        LIST_PREFIX_PAGES.put(
            continuationToken, Lists.newLinkedList(localStore.listObjects(bucketName, prefix)));
      }
      int keyCount =
          Math.min(LIST_OBJECT_MAX_KEYS, LIST_PREFIX_PAGES.get(continuationToken).size());
      List<ObjectMetadata> objects = LIST_PREFIX_PAGES.get(continuationToken).subList(0, keyCount);
      boolean isTruncated = LIST_PREFIX_PAGES.get(continuationToken).size() > keyCount;
      String result =
          generateListObjectsResponse(
              bucketName, prefix, continuationToken, keyCount, isTruncated, objects);
      updateListPrefixPages(continuationToken, keyCount);
      handleResponse(httpExchange, 200, result, "application/xml");
    }

    private String generateListObjectsResponse(
        String bucketName,
        String prefix,
        String continuationToken,
        int keyCount,
        boolean isTruncated,
        List<ObjectMetadata> objects) {
      // Generate XML response for ListObjectsV2
      StringBuilder xmlResponse = new StringBuilder();
      xmlResponse.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
      xmlResponse.append("<ListBucketResult>\n");
      xmlResponse.append("  <Name>").append(bucketName).append("</Name>\n");
      xmlResponse.append("  <Prefix>").append(prefix != null ? prefix : "").append("</Prefix>\n");
      xmlResponse.append("  <KeyCount>").append(keyCount).append("</KeyCount>\n");
      xmlResponse.append("  <MaxKeys>").append(LIST_OBJECT_MAX_KEYS).append("</MaxKeys>\n");
      xmlResponse.append("  <IsTruncated>").append(isTruncated).append("</IsTruncated>\n");
      if (isTruncated) {
        xmlResponse
            .append("  <NextContinuationToken>")
            .append(continuationToken)
            .append("</NextContinuationToken>\n");
      }
      for (ObjectMetadata metadata : objects) {
        xmlResponse.append("  <Contents>\n");
        xmlResponse
            .append("    <LastModified>")
            .append(
                createDate(
                    metadata.getLastModificationDate(), DateTimeFormatter.ISO_OFFSET_DATE_TIME))
            .append("</LastModified>\n");
        xmlResponse.append("    <Key>").append(metadata.getKey()).append("</Key>\n");
        xmlResponse.append("    <ETag>\"").append(metadata.getContentMD5()).append("\"</ETag>\n");
        xmlResponse.append("    <Size>").append(metadata.getContentLength()).append("</Size>\n");
        xmlResponse.append("    <StorageClass>STANDARD</StorageClass>\n");
        xmlResponse.append("  </Contents>\n");
      }
      xmlResponse.append("</ListBucketResult>\n");
      return xmlResponse.toString();
    }

    private void updateListPrefixPages(String continuationToken, int keyCount) {
      for (int i = 0; i < keyCount; i++) {
        LIST_PREFIX_PAGES.get(continuationToken).remove(0);
        if (LIST_PREFIX_PAGES.get(continuationToken).isEmpty()) {
          LIST_PREFIX_PAGES.remove(continuationToken);
        }
      }
    }

    private boolean verifyBucketExistence(String bucketName, HttpExchange httpExchange)
        throws IOException {
      Bucket bucket = localStore.getBucket(bucketName);
      if (bucket == null) {
        String errorMessage =
            createErrorResponse(
                OSSErrorCode.NO_SUCH_BUCKET, "The specified bucket does not exist.");
        handleResponse(httpExchange, 404, errorMessage, "application/xml");
        return false;
      }
      return true;
    }

    private ObjectMetadata verifyObjectExistence(String bucketName, String fileName) {
      ObjectMetadata objectMetadata = null;
      try {
        objectMetadata = localStore.getObjectMetadata(bucketName, fileName);
      } catch (IOException e) {
        // no-op
      }

      return objectMetadata;
    }

    private void handleResponse(
        HttpExchange httpExchange, int responseCode, String responsePayload, String contentType)
        throws IOException {
      OutputStream outputStream = httpExchange.getResponseBody();
      httpExchange.getResponseHeaders().put("Content-Type", Collections.singletonList(contentType));
      httpExchange.sendResponseHeaders(responseCode, responsePayload.length());
      outputStream.write(responsePayload.getBytes());
      outputStream.flush();
      outputStream.close();
    }

    private String createErrorResponse(String errorCode, String message) {
      StringBuilder builder = new StringBuilder();
      builder.append("<Error>");
      builder.append("<Code>").append(errorCode).append("</Code>");
      builder.append("<Message>").append(message).append("</Message>");
      builder.append("</Error>");
      return builder.toString();
    }

    private String createDate(long timestamp, DateTimeFormatter dateTimeFormatter) {
      java.util.Date date = new java.util.Date(timestamp);
      ZonedDateTime dateTime = date.toInstant().atZone(ZoneId.of("GMT"));
      return dateTime.format(dateTimeFormatter);
    }

    private void deleteObjects(String bucketName, HttpExchange httpExchange) throws IOException {
      boolean bucketExist = verifyBucketExistence(bucketName, httpExchange);
      if (!bucketExist) {
        return;
      }
      List<String> keysToDelete = parseDeleteObjectsRequest(httpExchange);
      List<String> deletedObjects = Lists.newArrayList();
      List<String> failedDeletions = Lists.newArrayList();
      for (String key : keysToDelete) {
        if (key.contains(SPECIAL_KEY_TRIGGER_MOCK_SERVER_ERROR)) {
          // mock server internal error causing one of the object deletion failed
          failedDeletions.add(key);
        } else {
          localStore.deleteObject(bucketName, key);
          deletedObjects.add(key);
        }
      }
      String xmlResponse = generateDeleteObjectsResponse(deletedObjects, failedDeletions);
      handleResponse(httpExchange, 200, xmlResponse, "application/xml");
    }

    private List<String> parseDeleteObjectsRequest(HttpExchange httpExchange) throws IOException {
      List<String> keys = Lists.newArrayList();
      try {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        String requestBody = new String(httpExchange.getRequestBody().readAllBytes());
        Document document = builder.parse(new InputSource(new StringReader(requestBody)));
        NodeList objectNodes = document.getElementsByTagName("Object");
        for (int i = 0; i < objectNodes.getLength(); i++) {
          Element objectElement = (Element) objectNodes.item(i);
          String key = objectElement.getElementsByTagName("Key").item(0).getTextContent();
          keys.add(key);
        }
      } catch (Exception e) {
        throw new IOException("Invalid XML in delete objects request", e);
      }
      return keys;
    }

    private String generateDeleteObjectsResponse(
        List<String> deletedObjects, List<String> failedDeletions) {
      StringBuilder xml = new StringBuilder();
      xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
      xml.append("<DeleteResult>\n");
      for (String key : deletedObjects) {
        xml.append("  <Deleted>\n");
        xml.append("    <Key>").append(key).append("</Key>\n");
        xml.append("  </Deleted>\n");
      }
      for (String key : failedDeletions) {
        xml.append("  <Error>\n");
        xml.append("    <Key>").append(key).append("</Key>\n");
        xml.append("    <Code>InternalError</Code>\n");
        xml.append("    <Message>Mock Internal Error</Message>\n");
        xml.append("  </Error>\n");
      }
      xml.append("</DeleteResult>\n");
      return xml.toString();
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
