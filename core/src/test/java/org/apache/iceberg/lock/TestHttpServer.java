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
package org.apache.iceberg.lock;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Map;
import java.util.Objects;
import java.util.StringTokenizer;
import org.apache.commons.collections.MapUtils;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHttpServer implements Runnable, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TestHttpServer.class);

  private static final int PORT = 28081;
  private ServerSocket server = null;
  private static final String RESP = "OK!";
  private static final String PATH_DEF = "/test-rest-lock?";
  private transient boolean running = true;
  private static final String OWNER_ID = "ownerId";
  private static final String ENTITY_ID = "entityId";
  private static final String OPERATOR = "operator";
  private static final String LOCK = "lock";
  private static final String UNLOCK = "unlock";
  private static final long TTL = 3600 * 1000 * 2;
  private final Map<String, String> cache = Maps.newConcurrentMap();

  public TestHttpServer() {
    try {
      server = new ServerSocket(PORT);
      new Thread(this).start();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      System.exit(1);
    }
  }

  private Map<String, String> extractParams(String paramList) {
    String[] params = paramList.split("&");
    Map<String, String> result = Maps.newHashMap();
    for (String paramKV : params) {
      String[] paramPair = paramKV.split("=");
      String key = paramPair[0];
      String value = paramPair[1];
      result.putIfAbsent(key, value);
    }
    return result;
  }

  private Map<String, String> parseHeader(BufferedReader reader) throws IOException {
    String line = null;
    Map<String, String> headers = Maps.newHashMap();
    while ((line = reader.readLine()) != null) {
      if (line.equals("")) {
        break;
      }
      String[] kvPair = line.split(":");
      headers.put(kvPair[0].trim(), kvPair[1].trim());
    }
    return headers;
  }

  @Override
  public void run() {
    while (running) {
      try (Socket client = server.accept()) {
        if (client == null) {
          LOG.info("skip.");
          continue;
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
        // GET /apple /HTTP1.1
        String line = reader.readLine();
        String resource = line.substring(line.indexOf('/'), line.lastIndexOf('/') - 5);
        resource = URLDecoder.decode(resource, "UTF-8");
        String method = new StringTokenizer(line).nextElement().toString();
        Map<String, String> headers = parseHeader(reader);
        long requestTimeStamp =
            MapUtils.getLongValue(headers, "timeStamp", System.currentTimeMillis());
        if (System.currentTimeMillis() - TTL > requestTimeStamp) {
          doResp(client, "HTTP/1.0 500 ERROR", null);
          continue;
        }
        if (!"GET".equalsIgnoreCase(method) || !resource.startsWith(PATH_DEF)) {
          doResp(client, "HTTP/1.0 404 Not found", null);
        } else {
          URI uri = URI.create(resource);
          String paramList = uri.getQuery();
          Map<String, String> paramKV = extractParams(paramList);
          String ownerId = paramKV.get(OWNER_ID);
          String entityId = paramKV.get(ENTITY_ID);
          String operator = paramKV.get(OPERATOR);
          switch (operator) {
            case LOCK:
              doLock(client, entityId, ownerId);
              break;
            case UNLOCK:
              cache.remove(entityId);
              doResp(client, "HTTP/1.0 200 OK", RESP);
              break;
            default:
              doResp(client, "HTTP/1.0 500 ERROR", null);
          }
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        throw new RuntimeException(e);
      }
    }
  }

  private void doLock(Socket client, String entityId, String ownerId) throws IOException {
    if (cache.containsKey(entityId)) {
      doResp(client, "HTTP/1.0 500 ERROR", null);
      return;
    }
    String realOwnerId = cache.putIfAbsent(entityId, ownerId);
    if (!Objects.equals(ownerId, realOwnerId) && realOwnerId != null) {
      doResp(client, "HTTP/1.0 500 ERROR", null);
    } else {
      doResp(client, "HTTP/1.0 200 OK", RESP);
    }
  }

  private void doResp(Socket client, String respLine, String body) throws IOException {
    PrintStream writer = new PrintStream(client.getOutputStream());
    writer.println(respLine);
    if (body != null) {
      writer.println("Content-Length:" + body.length());
    }
    writer.println();
    if (body != null) {
      writer.println(body);
    }
  }

  @Override
  public void close() throws IOException {
    running = false;
    if (server != null && !server.isClosed()) {
      server.close();
    }
  }
}
