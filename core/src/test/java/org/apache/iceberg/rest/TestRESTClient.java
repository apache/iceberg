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

package org.apache.iceberg.rest;

class TestRESTClient {
  // Placeholder
}

// import com.fasterxml.jackson.databind.ObjectMapper;
// import java.io.ByteArrayInputStream;
// import java.net.URI;
// import java.nio.charset.StandardCharsets;
// import java.util.concurrent.TimeUnit;
// import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
// import org.apache.hc.client5.http.entity.EntityBuilder;
// import org.apache.hc.client5.http.impl.classic.HttpClients;
// import org.junit.AfterClass;
// import org.junit.Assert;
// import org.junit.BeforeClass;
// import org.junit.Test;
// import org.mockserver.client.MockServerClient;
// import org.mockserver.integration.ClientAndServer;
// import org.mockserver.model.Header;
//
// import static org.mockserver.integration.ClientAndServer.startClientAndServer;
// import static org.mockserver.matchers.Times.exactly;
// import static org.mockserver.mock.OpenAPIExpectation.openAPIExpectation;
// import static org.mockserver.model.HttpRequest.request;
// import static org.mockserver.model.HttpResponse.response;
// import static org.mockserver.model.StringBody.subString;
//
// public class TestRESTClient {
//
//   private static final String host = "127.0.0.1";
//   private static final int port = 1080;
//   private static final String scheme = "http";
//   private static final String uri = String.format("%s://%s:%d/v1", scheme, host, port);
//   private static final ObjectMapper mapper = new ObjectMapper();
//
//   private static ClientAndServer mockServer;
//   private static RESTClient restClient;
//
//   @BeforeClass
//   public static void startServer() {
//     mockServer = startClientAndServer(port);
//     // mockServer.openUI();
//     restClient = HttpRESTClient
//         .builder()
//         .uri(uri)
//         .httpClient(HttpClients.createSystem())
//         .mapper(mapper)
//         .withBearerAuth("authtoken")
//         .build();
//   }
//
//   @AfterClass
//   public static void stopServer() {
//     mockServer.stop();
//   }
//
//   @Test
//   public void workingRawHTTPClientTest() throws Exception {
//     String json = "{}";
//     mockServer
//         .when(
//             request()
//                 .withMethod("POST")
//                 .withPath("/validate")
//                 .withHeaders(
//                     new Header("Content-Type", "application/json"),
//                     new Header("Accept", "application/json")
//                 )
//                 .withBody(json),
//             exactly(1))
//         .respond(
//             response()
//                 .withStatusCode(401)
//                 .withHeader("Content-Type", "application/json")
//                 .withBody("{ message: 'incorrect username and password combination' }")
//                 .withDelay(TimeUnit.SECONDS, 1)
//         );
//
//     HttpUriRequestBase request = new HttpUriRequestBase("POST", URI.create("http://127.0.0.1:1080/validate"));
//     request.setEntity(
//         EntityBuilder
//             .create()
//             .setStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))).build());
//     request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
//     request.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
//
//     Assert.assertEquals(
//         401,
//         HttpClients.createDefault().execute(request).getCode());
//   }
//
//   @Test
//   public void test200ResponseDoesNotHitErrorHandler() throws Exception {
//     // TODO - Loop over methods to test all (GET / HEAD / etc).
//     String reqJson = "{ testing: 'form not important' }";
//     String respJson = "{ testing: 'non-error response' }";
//     mockServer
//         .when(
//             request()
//                 .withMethod("POST")
//                 .withHeaders(
//                     new Header("Content-Type", "application/json"),
//                     new Header("Accept", "application/json"),
//                     new Header("Content-Length", String.valueOf(reqJson.getBytes(StandardCharsets.UTF_8).length))
//                 )
//                 .removeHeader("Accept-Encoding")
//                 .withPath("/v1/namespaces")
//                 .withBody(subString(reqJson)),
//             exactly(1))
//         .respond(
//             response()
//                 .withStatusCode(200)
//                 .withHeader("Content-Type", "application/json")
//                 .withBody(respJson)
//         );
//
//     // CreateNamespaceResponse mockedResponse =
//     //     restClient.post("namespaces", response, CreateNamespaceResponse.class);
//     // Assertions.assertThat(mockedResponse).isEqualTo(response);
//     // Assertions.assertThat(mockedResponse.namespace()).isEqualTo(response.namespace());
//   }
// }
