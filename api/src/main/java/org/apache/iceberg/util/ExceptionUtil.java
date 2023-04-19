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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ExceptionUtil.class);

  private ExceptionUtil() {}

  @SuppressWarnings("unchecked")
  public static <E extends Exception> void castAndThrow(
      Throwable exception, Class<E> exceptionClass) throws E {
    if (exception instanceof RuntimeException) {
      throw (RuntimeException) exception;
    } else if (exception instanceof Error) {
      throw (Error) exception;
    } else if (exceptionClass.isInstance(exception)) {
      throw (E) exception;
    }
    throw new RuntimeException(exception);
  }

  public interface Block<R, E1 extends Exception, E2 extends Exception, E3 extends Exception> {
    R run() throws E1, E2, E3;
  }

  public interface CatchBlock {
    void run(Throwable failure) throws Exception;
  }

  public interface FinallyBlock {
    void run() throws Exception;
  }

  public static <R> R runSafely(
      Block<R, RuntimeException, RuntimeException, RuntimeException> block,
      CatchBlock catchBlock,
      FinallyBlock finallyBlock) {
    return runSafely(
        block,
        catchBlock,
        finallyBlock,
        RuntimeException.class,
        RuntimeException.class,
        RuntimeException.class);
  }

  public static <R, E1 extends Exception> R runSafely(
      Block<R, E1, RuntimeException, RuntimeException> block,
      CatchBlock catchBlock,
      FinallyBlock finallyBlock,
      Class<? extends E1> e1Class)
      throws E1 {
    return runSafely(
        block, catchBlock, finallyBlock, e1Class, RuntimeException.class, RuntimeException.class);
  }

  public static <R, E1 extends Exception, E2 extends Exception> R runSafely(
      Block<R, E1, E2, RuntimeException> block,
      CatchBlock catchBlock,
      FinallyBlock finallyBlock,
      Class<? extends E1> e1Class,
      Class<? extends E2> e2Class)
      throws E1, E2 {
    return runSafely(block, catchBlock, finallyBlock, e1Class, e2Class, RuntimeException.class);
  }

  @SuppressWarnings("Finally")
  public static <R, E1 extends Exception, E2 extends Exception, E3 extends Exception> R runSafely(
      Block<R, E1, E2, E3> block,
      CatchBlock catchBlock,
      FinallyBlock finallyBlock,
      Class<? extends E1> e1Class,
      Class<? extends E2> e2Class,
      Class<? extends E3> e3Class)
      throws E1, E2, E3 {

    Throwable failure = null;
    try {
      return block.run();

    } catch (Throwable t) {
      failure = t;

      if (catchBlock != null) {
        try {
          catchBlock.run(failure);
        } catch (Exception e) {
          LOG.warn("Suppressing failure in catch block", e);
          failure.addSuppressed(e);
        }
      }

      tryThrowAs(failure, e1Class);
      tryThrowAs(failure, e2Class);
      tryThrowAs(failure, e3Class);
      tryThrowAs(failure, RuntimeException.class);
      throw new RuntimeException("Unknown throwable", failure);

    } finally {
      if (finallyBlock != null) {
        try {
          finallyBlock.run();
        } catch (Exception e) {
          if (failure != null) {
            LOG.warn("Suppressing failure in finally block", e);
            failure.addSuppressed(e);
          } else {
            tryThrowAs(e, e1Class);
            tryThrowAs(e, e2Class);
            tryThrowAs(e, e3Class);
            tryThrowAs(e, RuntimeException.class);
            throw new RuntimeException("Unknown exception in finally block", e);
          }
        }
      }
    }
  }

  private static <E extends Exception> void tryThrowAs(Throwable failure, Class<E> excClass)
      throws E {
    if (excClass.isInstance(failure)) {
      throw excClass.cast(failure);
    }
  }
}
