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

package org.apache.iceberg.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tasks {
  private static final Logger LOG = LoggerFactory.getLogger(Tasks.class);

  private Tasks() {
  }

  public static class UnrecoverableException extends RuntimeException {
    public UnrecoverableException(String message) {
      super(message);
    }

    public UnrecoverableException(String message, Throwable cause) {
      super(message, cause);
    }

    public UnrecoverableException(Throwable cause) {
      super(cause);
    }
  }

  public interface FailureTask<I, E extends Exception> {
    void run(I item, Exception exception) throws E;
  }

  public interface Task<I, E extends Exception> {
    void run(I item) throws E;
  }

  public static class Builder<I> extends Calls.BaseBuilder<I, Void, Builder<I>> {

    public Builder(Iterable<I> items) {
      super(items);
    }

    public Builder(I item) {
      super(item);
    }

    public Builder<I> revertWith(Task<I, ?> task) {
      revertWith((i, o) -> task.run(i));
      return this;
    }

    public boolean run(Task<I, RuntimeException> task) {
      return run(task, RuntimeException.class);
    }

    public <E extends Exception> boolean run(Task<I, E> task, Class<E> runtimeExceptionClass) throws E {
      Stream<Void> result = doCall(i -> {
        task.run(i);
        return null;
      }, runtimeExceptionClass);
      return result != null;
    }
  }

  public static Builder<Integer> range(int upTo) {
    return new Builder<>(new Calls.Range(upTo));
  }

  public static <I> Builder<I> foreach(Iterator<I> items) {
    return new Builder<>(() -> items);
  }

  public static <I> Builder<I> foreach(Iterable<I> items) {
    return new Builder<>(items);
  }

  public static <I> Builder<I> foreach(I... items) {
    return new Builder<>(Arrays.asList(items));
  }
}
