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
package org.apache.iceberg.io;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.resource.ResourceFactory;

/** Serves a directory over HTTP with range requests, standing in for an object store. */
public class PreSignedUrlTestServer implements AutoCloseable {

  private final Path root;
  private final Server server;
  private final List<String> ranges = new CopyOnWriteArrayList<>();

  /** Hide the Range header from the server: every GET answers 200 with the whole object. */
  public volatile boolean ignoreRange = false;

  public PreSignedUrlTestServer(Path root) throws Exception {
    this.root = Files.createDirectories(root).toRealPath();
    this.server = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    ResourceHandler files = new ResourceHandler();
    files.setBaseResource(ResourceFactory.of(server).newResource(this.root));
    server.setHandler(
        new Handler.Wrapper(files) {
          @Override
          public boolean handle(Request request, Response response, Callback callback)
              throws Exception {
            ranges.add(request.getHeaders().get(HttpHeader.RANGE));
            return super.handle(ignoreRange ? withoutRange(request) : request, response, callback);
          }
        });
    server.start();
  }

  public void put(String key, byte[] data) {
    try {
      Path file = root.resolve(key);
      Files.createDirectories(file.getParent());
      Files.write(file, data);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public String url(String key) {
    return server.getURI() + key + "?X-Test-Signature=1";
  }

  /** The {@code Range} header of each request, in order. */
  public List<String> ranges() {
    return ranges;
  }

  @Override
  public void close() throws Exception {
    server.stop();
  }

  private static Request withoutRange(Request request) {
    return new Request.Wrapper(request) {
      @Override
      public HttpFields getHeaders() {
        return HttpFields.build(super.getHeaders()).remove(HttpHeader.RANGE).asImmutable();
      }
    };
  }
}
