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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.RequestAcceptEncoding;
import org.apache.http.client.protocol.ResponseContentEncoding;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.LockManagers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The user is required to provide an API interface with distributed locking functionality as
 * agreed.
 */
public class ServerSideHttpLockManager extends LockManagers.BaseLockManager {
  private static final Logger LOG = LoggerFactory.getLogger(ServerSideHttpLockManager.class);
  private String httpUrl = null;
  private static final String OPERATOR = "operator";
  private static final String LOCK = "lock";
  private static final String UNLOCK = "unlock";
  private static final String ENTITY_ID = "entityId";
  private static final String OWNER_ID = "ownerId";
  private static final int REQUEST_SUCCESS = 200;
  public static final String REQUEST_URL = "lock.http.conf.request.url";
  public static final String REQUEST_AUTH = "lock.http.conf.request.auth.impl";
  private HttpClient httpClient = null;
  private HttpAuthentication httpAuthentication = null;

  public ServerSideHttpLockManager() {}

  public ServerSideHttpLockManager(String requestUrl) {
    initialize(ImmutableMap.of(REQUEST_URL, requestUrl));
  }

  @Override
  public boolean acquire(String entityId, String ownerId) {
    return process(entityId, ownerId, LOCK);
  }

  @Override
  public void initialize(Map<String, String> properties) {
    super.initialize(properties);
    init(properties);
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (httpClient != null) {
      httpClient.getConnectionManager().shutdown();
    }
  }

  private synchronized void init(Map<String, String> properties) {
    String requestUrl = properties.getOrDefault(REQUEST_URL, null);
    String authImplClass = properties.getOrDefault(REQUEST_AUTH, null);
    if (requestUrl == null) {
      String msg = String.format("[%s] must be set.", REQUEST_URL);
      throw new IllegalArgumentException(msg);
    }
    if (this.httpUrl == null) {
      this.httpUrl = requestUrl;
    }
    try {
      if (this.httpClient == null) {
        DefaultHttpClient defaultHttpClient = new DefaultHttpClient();
        defaultHttpClient.addRequestInterceptor(new RequestAcceptEncoding());
        defaultHttpClient.addResponseInterceptor(new ResponseContentEncoding());
        defaultHttpClient
            .getParams()
            .setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, acquireTimeoutMs());
        defaultHttpClient
            .getParams()
            .setParameter(CoreConnectionPNames.SO_TIMEOUT, heartbeatTimeoutMs());
        this.httpClient = new DefaultHttpClient();
      }
      if (authImplClass == null) {
        httpAuthentication = new DefaultAuthImpl();
      } else {
        DynConstructors.Ctor<HttpAuthentication> ctor =
            DynConstructors.builder(HttpAuthentication.class)
                .hiddenImpl(authImplClass)
                .buildChecked();
        httpAuthentication = ctor.newInstance();
      }
      httpAuthentication.init(properties);
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  private String encode(String entity) {
    if (entity == null) {
      return null;
    }
    return new String(
        Base64.getUrlEncoder().encode(entity.getBytes(StandardCharsets.UTF_8)),
        StandardCharsets.UTF_8);
  }

  private boolean process(String entityId, String ownerId, String operator) {
    try {
      HttpGet lockRequest = new HttpGet();
      lockRequest.addHeader("Content-Type", "application/json");
      String requestUrl =
          new URIBuilder(httpUrl)
              .setParameter(OWNER_ID, encode(ownerId))
              .setParameter(ENTITY_ID, encode(entityId))
              .setParameter(OPERATOR, operator)
              .build()
              .toString();
      Map<String, String> headerMap = httpAuthentication.assignRequestHeader(requestUrl);
      if (headerMap != null) {
        headerMap.forEach(lockRequest::addHeader);
      }
      lockRequest.setURI(new URI(requestUrl));
      HttpResponse response = httpClient.execute(lockRequest);
      StatusLine stateLine = response.getStatusLine();
      int statsCode = stateLine.getStatusCode();
      String content = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
      lockRequest.abort();
      boolean respStatIsOk = REQUEST_SUCCESS == statsCode;
      if (respStatIsOk) {
        boolean confirmSuccess = httpAuthentication.confirmResponse(content);
        if (!confirmSuccess) {
          LOG.error("Request not success.Resp is [{}]", content);
        }
        return confirmSuccess;
      } else {
        return false;
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    } catch (Exception e) {
      httpClient.getConnectionManager().closeExpiredConnections();
      LOG.error("An exception occurred during the {} process.", operator, e);
    }
    return false;
  }

  @Override
  public boolean release(String entityId, String ownerId) {
    return process(entityId, ownerId, UNLOCK);
  }
}
