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
package org.apache.iceberg.io.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.junit.jupiter.api.Test;

class TestHttpHeaderUtil {

  @Test
  void parseTotalFromContentRangeReadsTotalAfterSlash() {
    assertThat(HttpHeaderUtil.parseTotalFromContentRange("bytes 0-0/12345")).isEqualTo(12345L);
    assertThat(HttpHeaderUtil.parseTotalFromContentRange("bytes 100-199/12345")).isEqualTo(12345L);
  }

  @Test
  void parseTotalFromContentRangeTrimsWhitespaceAroundTotal() {
    assertThat(HttpHeaderUtil.parseTotalFromContentRange("bytes 0-0/ 12345 ")).isEqualTo(12345L);
  }

  @Test
  void parseTotalFromContentRangeReturnsUnknownForUnsatisfiedTotal() {
    assertThat(HttpHeaderUtil.parseTotalFromContentRange("bytes 0-0/*"))
        .isEqualTo(HttpHeaderUtil.UNKNOWN_LENGTH);
  }

  @Test
  void parseTotalFromContentRangeReturnsUnknownWithoutSlash() {
    assertThat(HttpHeaderUtil.parseTotalFromContentRange("bytes 0-0"))
        .isEqualTo(HttpHeaderUtil.UNKNOWN_LENGTH);
  }

  @Test
  void parseTotalFromContentRangeReturnsUnknownForNonNumericTotal() {
    assertThat(HttpHeaderUtil.parseTotalFromContentRange("bytes 0-0/abc"))
        .isEqualTo(HttpHeaderUtil.UNKNOWN_LENGTH);
  }

  @Test
  void parseTotalFromPartialContentReadsContentRangeHeader() {
    ClassicHttpResponse response = new BasicClassicHttpResponse(HttpStatus.SC_PARTIAL_CONTENT);
    response.setHeader("Content-Range", "bytes 0-0/2048");

    assertThat(HttpHeaderUtil.parseTotalFromPartialContent(response)).isEqualTo(2048L);
  }

  @Test
  void parseTotalFromPartialContentReturnsUnknownWithoutContentRangeHeader() {
    ClassicHttpResponse response = new BasicClassicHttpResponse(HttpStatus.SC_PARTIAL_CONTENT);

    assertThat(HttpHeaderUtil.parseTotalFromPartialContent(response))
        .isEqualTo(HttpHeaderUtil.UNKNOWN_LENGTH);
  }

  @Test
  void parseTotalFromPartialContentReturnsUnknownForMalformedContentRange() {
    ClassicHttpResponse response = new BasicClassicHttpResponse(HttpStatus.SC_PARTIAL_CONTENT);
    response.setHeader("Content-Range", "bytes 0-0/*");

    assertThat(HttpHeaderUtil.parseTotalFromPartialContent(response))
        .isEqualTo(HttpHeaderUtil.UNKNOWN_LENGTH);
  }

  @Test
  void parseLengthFrom200PrefersEntityContentLength() {
    byte[] body = "hello".getBytes(StandardCharsets.UTF_8);
    ClassicHttpResponse response = new BasicClassicHttpResponse(HttpStatus.SC_OK);
    response.setEntity(new ByteArrayEntity(body, ContentType.APPLICATION_OCTET_STREAM));

    assertThat(HttpHeaderUtil.parseLengthFrom200(response)).isEqualTo(body.length);
  }

  @Test
  void parseLengthFrom200FallsBackToContentLengthHeader() {
    ClassicHttpResponse response = new BasicClassicHttpResponse(HttpStatus.SC_OK);
    response.setHeader("Content-Length", "4096");

    assertThat(HttpHeaderUtil.parseLengthFrom200(response)).isEqualTo(4096L);
  }

  @Test
  void parseLengthFrom200ReturnsUnknownWithoutEntityOrHeader() {
    ClassicHttpResponse response = new BasicClassicHttpResponse(HttpStatus.SC_OK);

    assertThat(HttpHeaderUtil.parseLengthFrom200(response))
        .isEqualTo(HttpHeaderUtil.UNKNOWN_LENGTH);
  }

  @Test
  void parseLengthFrom200ReturnsUnknownForNonNumericContentLengthHeader() {
    ClassicHttpResponse response = new BasicClassicHttpResponse(HttpStatus.SC_OK);
    response.setHeader("Content-Length", "not-a-number");

    assertThat(HttpHeaderUtil.parseLengthFrom200(response))
        .isEqualTo(HttpHeaderUtil.UNKNOWN_LENGTH);
  }
}
