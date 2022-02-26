/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.rest;

import java.util.function.Consumer;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class RESTClientTest {

  private static RESTClient restClient;
  private static Consumer<ErrorResponse> errorResponseConsumer;

  @BeforeClass
  public static void setUp() throws Exception {
    restClient = Mockito.mock(RESTClient.class);

  }

  @Test
  public void testDelete() {
    String mockDeleteResponse = "DELETED";
    Mockito.when(restClient.delete(Mockito.eq("/delete"), Mockito.any())).thenReturn(mockDeleteResponse);

    String mockDeleteCall = restClient.delete("/delete", String.class);
    Assert.assertEquals("")
  }

  @Test
  public void post() {
  }

  @Test
  public void testPost() {
  }

  @Test
  public void get() {
  }

  @Test
  public void testGet() {
  }

  @Test
  public void head() {
  }

  @Test
  public void testHead() {
  }
}