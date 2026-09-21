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
package org.apache.iceberg.spark;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PreSignedUrlInputFile;
import org.apache.iceberg.io.SupportsPreSigning;
import org.apache.iceberg.rest.RemoteSigningClient;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import org.apache.iceberg.util.SerializableMap;

/** A FileIO whose only capability is signing through the catalog, for tests. */
public class SigningFileIO implements FileIO, SupportsPreSigning {
  private SerializableMap<String, String> properties;
  private transient volatile RemoteSigningClient signingClient;

  @Override
  public void initialize(Map<String, String> props) {
    this.properties = SerializableMap.copyOf(props);
  }

  @Override
  public Map<String, String> properties() {
    return properties.immutableMap();
  }

  @Override
  public Map<String, RemoteSignResponse> preSign(Collection<String> locations) {
    return signingClient().preSign(locations, SigningFileIO::remoteSignRequest);
  }

  private static RemoteSignRequest remoteSignRequest(String location) {
    return ImmutableRemoteSignRequest.builder()
        .method("GET")
        .region("us-east-1")
        .uri(URI.create(location))
        .provider("s3")
        .build();
  }

  private RemoteSigningClient signingClient() {
    if (null == signingClient) {
      synchronized (this) {
        if (null == signingClient) {
          this.signingClient = RemoteSigningClient.create(properties);
        }
      }
    }

    return signingClient;
  }

  @Override
  public InputFile newInputFile(String location) {
    return PreSignedUrlInputFile.of(location, 0);
  }

  @Override
  public OutputFile newOutputFile(String location) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteFile(String location) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    if (null != signingClient) {
      signingClient.close();
    }
  }
}
