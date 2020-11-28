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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Calls {
  private static final Logger LOG = LoggerFactory.getLogger(Calls.class);

  private Calls() {
  }

  public interface RevertTask<I, O, E extends Exception> {
    void run(I item, O result) throws E;
  }

  public interface Call<I, O, E extends Exception> {
    O call(I item) throws E;
  }

  public static class Builder<I, O> extends BaseBuilder<I, O, Builder<I, O>> {
    public Builder(Iterable<I> items) {
      super(items);
    }

    public Builder(I item) {
      super(item);
    }

    public Stream<O> call(Call<I, O, RuntimeException> call) {
      return doCall(call);
    }

    public <E extends Exception> Stream<O> call(Call<I, O, E> call, Class<E> runtimeExceptionClass) throws E {
      return doCall(call, runtimeExceptionClass);
    }

    public O first(Call<I, O, RuntimeException> call) {
      return first(call, RuntimeException.class);
    }

    public <E extends Exception> O first(Call<I, O, E> call, Class<E> runtimeExceptionClass) throws E {
      Stream<O> result = doCall(call, runtimeExceptionClass);
      if (result == null) {
        return null;
      } else {
        return result.findFirst().get();
      }
    }
  }

  public static class BaseBuilder<I, O, T extends BaseBuilder<I, O, T>> {
    private final Iterable<I> items;
    private ExecutorService service = null;
    private Tasks.FailureTask<I, ?> onFailure = null;
    private boolean stopOnFailure = false;
    private boolean throwFailureWhenFinished = true;
    private RevertTask<I, O, ?> revertTask = null;
    private boolean stopRevertsOnFailure = false;
    private Tasks.Task<I, ?> abortTask = null;
    private boolean stopAbortsOnFailure = false;

    // retry settings
    @SuppressWarnings("unchecked")
    private List<Class<? extends Exception>> stopRetryExceptions = Lists.newArrayList(
        Tasks.UnrecoverableException.class);
    private List<Class<? extends Exception>> onlyRetryExceptions = null;
    private Predicate<Exception> shouldRetryPredicate = null;
    private int maxAttempts = 1;          // not all operations can be retried
    private long minSleepTimeMs = 1000;   // 1 second
    private long maxSleepTimeMs = 600000; // 10 minutes
    private long maxDurationMs = 600000;  // 10 minutes
    private double scaleFactor = 2.0;     // exponential

    public BaseBuilder(I items) {
      this.items = Collections.singleton(items);
    }

    public BaseBuilder(Iterable<I> items) {
      this.items = items;
    }

    private static class Pair<I, O> {
      private final I item;
      private final O result;

      private Pair(I item, O result) {
        this.item = item;
        this.result = result;
      }

      public static <I, O> Pair<I, O> of(I item, O result) {
        return new Pair<>(item, result);
      }

      public I item() {
        return item;
      }

      public O result() {
        return result;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (!(o instanceof Pair)) {
          return false;
        }
        Pair<?, ?> that = (Pair<?, ?>) o;
        return item.equals(that.item) &&
            result.equals(that.result);
      }

      @Override
      public int hashCode() {
        return Objects.hash(item, result);
      }
    }

    public T executeWith(ExecutorService svc) {
      this.service = svc;
      return (T) this;
    }

    public T onFailure(Tasks.FailureTask<I, ?> task) {
      this.onFailure = task;
      return (T) this;
    }

    public T stopOnFailure() {
      this.stopOnFailure = true;
      return (T) this;
    }

    public T throwFailureWhenFinished() {
      this.throwFailureWhenFinished = true;
      return (T) this;
    }

    public T throwFailureWhenFinished(boolean throwWhenFinished) {
      this.throwFailureWhenFinished = throwWhenFinished;
      return (T) this;
    }

    public T suppressFailureWhenFinished() {
      this.throwFailureWhenFinished = false;
      return (T) this;
    }

    public T revertWith(RevertTask<I, O, ?> task) {
      this.revertTask = task;
      return (T) this;
    }

    public T stopRevertsOnFailure() {
      this.stopRevertsOnFailure = true;
      return (T) this;
    }

    public T abortWith(Tasks.Task<I, ?> call) {
      this.abortTask = call;
      return (T) this;
    }

    public T stopAbortsOnFailure() {
      this.stopAbortsOnFailure = true;
      return (T) this;
    }

    public T stopRetryOn(Class<? extends Exception>... exceptions) {
      stopRetryExceptions.addAll(Arrays.asList(exceptions));
      return (T) this;
    }

    public T shouldRetryTest(Predicate<Exception> shouldRetry) {
      this.shouldRetryPredicate = shouldRetry;
      return (T) this;
    }

    public T noRetry() {
      this.maxAttempts = 1;
      return (T) this;
    }

    public T retry(int nTimes) {
      this.maxAttempts = nTimes + 1;
      return (T) this;
    }

    public T onlyRetryOn(Class<? extends Exception> exception) {
      this.onlyRetryExceptions = Collections.singletonList(exception);
      return (T) this;
    }

    public T onlyRetryOn(Class<? extends Exception>... exceptions) {
      this.onlyRetryExceptions = Lists.newArrayList(exceptions);
      return (T) this;
    }

    public T exponentialBackoff(long backoffMinSleepTimeMs,
                                         long backoffMaxSleepTimeMs,
                                         long backoffMaxRetryTimeMs,
                                         double backoffScaleFactor) {
      this.minSleepTimeMs = backoffMinSleepTimeMs;
      this.maxSleepTimeMs = backoffMaxSleepTimeMs;
      this.maxDurationMs = backoffMaxRetryTimeMs;
      this.scaleFactor = backoffScaleFactor;
      return (T) this;
    }

    protected Stream<O> doCall(Call<I, O, RuntimeException> call) {
      return doCall(call, RuntimeException.class);
    }

    protected <E extends Exception> Stream<O> doCall(Call<I, O, E> call,
                                                Class<E> exceptionClass) throws E {
      Stream<Pair<I, O>> stream;
      if (service != null) {
        stream = runParallel(call, exceptionClass);
      } else {
        stream = runSingleThreaded(call, exceptionClass);
      }
      if (stream == null) {
        return null;
      } else {
        return stream.map(Pair::result);
      }
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private <E extends Exception> Stream<Pair<I, O>> runSingleThreaded(
        Call<I, O, E> call, Class<E> exceptionClass) throws E {
      List<Pair<I, O>> succeeded = Lists.newArrayList();
      List<Throwable> exceptions = Lists.newArrayList();

      Iterator<I> iterator = items.iterator();
      boolean threw = true;
      try {
        while (iterator.hasNext()) {
          I item = iterator.next();
          try {
            O result = runCallWithRetry(call, item);
            succeeded.add(Pair.of(item, result));

          } catch (Exception e) {
            exceptions.add(e);

            if (onFailure != null) {
              tryRunOnFailure(item, e);
            }

            if (stopOnFailure) {
              break;
            }
          }
        }

        threw = false;

      } finally {
        // threw handles exceptions that were *not* caught by the catch block,
        // and exceptions that were caught and possibly handled by onFailure
        // are kept in exceptions.
        if (threw || !exceptions.isEmpty()) {
          if (revertTask != null) {
            boolean failed = false;
            for (Pair<I, O> itemResult : succeeded) {
              try {
                revertTask.run(itemResult.item(), itemResult.result());
              } catch (Exception e) {
                failed = true;
                LOG.error("Failed to revert call", e);
                // keep going
              }
              if (stopRevertsOnFailure && failed) {
                break;
              }
            }
          }

          if (abortTask != null) {
            boolean failed = false;
            while (iterator.hasNext()) {
              try {
                abortTask.run(iterator.next());
              } catch (Exception e) {
                failed = true;
                LOG.error("Failed to abort call", e);
                // keep going
              }
              if (stopAbortsOnFailure && failed) {
                break;
              }
            }
          }
        }
      }

      if (throwFailureWhenFinished && !exceptions.isEmpty()) {
        Calls.throwOne(exceptions, exceptionClass);
      } else if (throwFailureWhenFinished && threw) {
        throw new RuntimeException(
            "Call set failed with an uncaught throwable");
      }

      if (threw) {
        return null;
      } else {
        return succeeded.stream();
      }
    }

    private void tryRunOnFailure(I item, Exception failure) {
      try {
        onFailure.run(item, failure);
      } catch (Exception failException) {
        failure.addSuppressed(failException);
        LOG.error("Failed to clean up on failure", failException);
        // keep going
      }
    }

    private <E extends Exception> Stream<Pair<I, O>> runParallel(final Call<I, O, E> call,
                                                        Class<E> exceptionClass)
        throws E {
      final Queue<Pair<I, O>> succeeded = new ConcurrentLinkedQueue<>();
      final Queue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
      final AtomicBoolean callFailed = new AtomicBoolean(false);
      final AtomicBoolean abortFailed = new AtomicBoolean(false);
      final AtomicBoolean revertFailed = new AtomicBoolean(false);

      List<Future<?>> futures = Lists.newArrayList();

      for (final I item : items) {
        // submit a call for each item that will either run or abort the call
        futures.add(service.submit(new Runnable() {
          @Override
          public void run() {
            if (!(stopOnFailure && callFailed.get())) {
              // run the call with retries
              boolean threw = true;
              try {
                O result = runCallWithRetry(call, item);

                succeeded.add(Pair.of(item, result));

                threw = false;

              } catch (Exception e) {
                callFailed.set(true);
                exceptions.add(e);

                if (onFailure != null) {
                  tryRunOnFailure(item, e);
                }
              } finally {
                if (threw) {
                  callFailed.set(true);
                }
              }

            } else if (abortTask != null) {
              // abort the call instead of running it
              if (stopAbortsOnFailure && abortFailed.get()) {
                return;
              }

              boolean failed = true;
              try {
                abortTask.run(item);
                failed = false;
              } catch (Exception e) {
                LOG.error("Failed to abort call", e);
                // swallow the exception
              } finally {
                if (failed) {
                  abortFailed.set(true);
                }
              }
            }
          }
        }));
      }

      // let the above calls complete (or abort)
      exceptions.addAll(waitFor(futures));
      futures.clear();

      if (callFailed.get() && revertTask != null) {
        // at least one call failed, revert any that succeeded
        for (final Pair<I, O> itemResult : succeeded) {
          futures.add(service.submit(new Runnable() {
            @Override
            public void run() {
              if (stopRevertsOnFailure && revertFailed.get()) {
                return;
              }

              boolean failed = true;
              try {
                revertTask.run(itemResult.item(), itemResult.result());
                failed = false;
              } catch (Exception e) {
                LOG.error("Failed to revert call", e);
                // swallow the exception
              } finally {
                if (failed) {
                  revertFailed.set(true);
                }
              }
            }
          }));
        }

        // let the revert calls complete
        exceptions.addAll(waitFor(futures));
      }

      if (throwFailureWhenFinished && !exceptions.isEmpty()) {
        Calls.throwOne(exceptions, exceptionClass);
      } else if (throwFailureWhenFinished && callFailed.get()) {
        throw new RuntimeException(
            "Call set failed with an uncaught throwable");
      }

      if (callFailed.get()) {
        return null;
      } else {
        return succeeded.parallelStream();
      }
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private <E extends Exception> O runCallWithRetry(Call<I, O, E> call, I item)
        throws E {
      long start = System.currentTimeMillis();
      int attempt = 0;
      while (true) {
        attempt += 1;
        try {
          O result = call.call(item);
          return result;
        } catch (Exception e) {
          long durationMs = System.currentTimeMillis() - start;
          if (attempt >= maxAttempts || (durationMs > maxDurationMs && attempt > 1)) {
            if (durationMs > maxDurationMs) {
              LOG.info("Stopping retries after {} ms", durationMs);
            }
            throw e;
          }

          if (shouldRetryPredicate != null) {
            if (!shouldRetryPredicate.test(e)) {
              throw e;
            }

          } else if (onlyRetryExceptions != null) {
            // if onlyRetryExceptions are present, then this retries if one is found
            boolean matchedRetryException = false;
            for (Class<? extends Exception> exClass : onlyRetryExceptions) {
              if (exClass.isInstance(e)) {
                matchedRetryException = true;
                break;
              }
            }
            if (!matchedRetryException) {
              throw e;
            }

          } else {
            // otherwise, always retry unless one of the stop exceptions is found
            for (Class<? extends Exception> exClass : stopRetryExceptions) {
              if (exClass.isInstance(e)) {
                throw e;
              }
            }
          }

          int delayMs = (int) Math.min(
              minSleepTimeMs * Math.pow(scaleFactor, attempt - 1),
              maxSleepTimeMs);
          int jitter = ThreadLocalRandom.current()
              .nextInt(Math.max(1, (int) (delayMs * 0.1)));

          LOG.warn("Retrying call after failure: {}", e.getMessage(), e);

          try {
            TimeUnit.MILLISECONDS.sleep(delayMs + jitter);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
          }
        }
      }
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static Collection<Throwable> waitFor(Collection<Future<?>> futures) {
    while (true) {
      int numFinished = 0;
      for (Future<?> future : futures) {
        if (future.isDone()) {
          numFinished += 1;
        }
      }

      if (numFinished == futures.size()) {
        List<Throwable> uncaught = new ArrayList<>();
        // all of the futures are done, get any uncaught exceptions
        for (Future<?> future : futures) {
          try {
            future.get();

          } catch (InterruptedException e) {
            LOG.warn("Interrupted while getting future results", e);
            for (Throwable t : uncaught) {
              e.addSuppressed(t);
            }
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);

          } catch (CancellationException e) {
            // ignore cancellations

          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (Error.class.isInstance(cause)) {
              for (Throwable t : uncaught) {
                cause.addSuppressed(t);
              }
              throw (Error) cause;
            }

            if (cause != null) {
              uncaught.add(e);
            }

            LOG.warn("Call threw uncaught exception", cause);
          }
        }

        return uncaught;

      } else {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for calls to finish", e);

          for (Future<?> future : futures) {
            future.cancel(true);
          }
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * A range, [ 0, size )
   */
  public static class Range implements Iterable<Integer> {
    private int size;

    Range(int size) {
      this.size = size;
    }

    @Override
    public Iterator<Integer> iterator() {
      return new Iterator<Integer>() {
        private int current = 0;

        @Override
        public boolean hasNext() {
          return current < size;
        }

        @Override
        public Integer next() {
          int ret = current;
          current += 1;
          return ret;
        }
      };
    }
  }

  @SuppressWarnings("unchecked")
  private static <E extends Exception> void throwOne(
      Collection<Throwable> exceptions, Class<E> allowedException) throws E {
    Iterator<Throwable> iter = exceptions.iterator();
    Throwable exception = iter.next();
    Class<? extends Throwable> exceptionClass = exception.getClass();

    while (iter.hasNext()) {
      Throwable other = iter.next();
      if (!exceptionClass.isInstance(other)) {
        exception.addSuppressed(other);
      }
    }

    ExceptionUtil.castAndThrow(exception, allowedException);
  }
  public static <O> Builder<Integer, O> range(int upTo) {
    return new Builder<>(new Range(upTo));
  }

  public static <I, O> Builder<I, O> foreach(Iterator<I> items) {
    return new Builder<>(() -> items);
  }

  public static <I, O> Builder<I, O> foreach(Iterable<I> items) {
    return new Builder<>(items);
  }

  public static <I, O> Builder<I, O> foreach(I... items) {
    return new Builder<>(Arrays.asList(items));
  }
}
