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
package org.apache.iceberg.azure.adlsv2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockserver.model.HttpRequest.request;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpMethod;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import org.junit.jupiter.api.Test;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;
import reactor.core.publisher.Mono;

public class TestVendedAzureSasCredentialPolicy extends VendedCredentialsTestBase {

  private static final String STORAGE_ACCOUNT = "account1";
  private static final String ACCOUNT_ENDPOINT = String.format("%s/%s", baseUri, STORAGE_ACCOUNT);

  @Test
  public void vendedSasTokenAsRequestQueryParameters() {
    String filePath = "file1";
    String container = "container1";
    String validSasToken = "tokenInstance=1";
    String expiredSasToken = "tokenInstance=2";

    VendedAdlsCredentialProvider vendedAdlsCredentialProvider =
        mock(VendedAdlsCredentialProvider.class);
    VendedAzureSasCredentialPolicy vendedAzureSasCredentialPolicy =
        new VendedAzureSasCredentialPolicy(STORAGE_ACCOUNT, vendedAdlsCredentialProvider);

    DataLakeFileSystemClient client =
        new DataLakeFileSystemClientBuilder()
            .httpClient(HttpClient.createDefault())
            .addPolicy(vendedAzureSasCredentialPolicy)
            .fileSystemName(container)
            .endpoint(ACCOUNT_ENDPOINT)
            .buildClient();

    String requestPath = String.format("/%s/%s/%s", STORAGE_ACCOUNT, container, filePath);

    HttpRequest mockRequestWithValidSasToken =
        request(requestPath)
            .withMethod(HttpMethod.HEAD.name())
            .withQueryStringParameter("tokenInstance", "1");
    mockServer
        .when(mockRequestWithValidSasToken)
        .respond(HttpResponse.response().withStatusCode(200));

    HttpRequest mockRequestWithExpiredSasToken =
        request(requestPath)
            .withMethod(HttpMethod.HEAD.name())
            .withQueryStringParameter("tokenInstance", "2");
    mockServer
        .when(mockRequestWithExpiredSasToken)
        .respond(HttpResponse.response().withStatusCode(403));

    when(vendedAdlsCredentialProvider.credentialForAccount(STORAGE_ACCOUNT))
        .thenReturn(Mono.just(validSasToken));
    assertThat(client.getFileClient(filePath).exists()).isTrue();
    mockServer.verify(mockRequestWithValidSasToken, VerificationTimes.exactly(1));

    when(vendedAdlsCredentialProvider.credentialForAccount(STORAGE_ACCOUNT))
        .thenReturn(Mono.just(expiredSasToken));

    // Every new request of the same client fetches latest SasToken credentials from
    // VendedAdlsCredentialProvider to build http request query parameters.
    assertThatThrownBy(() -> client.getFileClient(filePath).exists())
        .isInstanceOf(DataLakeStorageException.class)
        .hasMessageContaining(
            "If you are using a SAS token, and the server returned an error message that says 'Signature did not match'");
    mockServer.verify(mockRequestWithExpiredSasToken, VerificationTimes.atLeast(1));
  }
}
