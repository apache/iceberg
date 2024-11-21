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
package org.apache.iceberg.aws;

import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.auth.OAuth2Util;

/**
 * An AuthSession that signs requests with SigV4.
 *
 * <p>It extends {@link OAuth2Util.AuthSession} to handle OAuth2 authentication as well.
 */
public class RESTSigv4AuthSession extends OAuth2Util.AuthSession {

  private final RESTSigV4Signer signer;

  public RESTSigv4AuthSession(OAuth2Util.AuthSession authSession, RESTSigV4Signer signer) {
    super(authSession.headers(), authSession.config());
    this.signer = signer;
  }

  @Override
  public void authenticate(HTTPRequest.Builder request) {
    super.authenticate(request);
    signer.sign(request);
  }
}
