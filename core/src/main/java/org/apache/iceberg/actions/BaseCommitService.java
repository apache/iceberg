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
package org.apache.iceberg.actions;

import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An async service which allows for committing multiple file groups as their rewrites complete. The
 * service also allows for partial-progress since commits can fail. Once the service has been closed
 * no new file groups should not be offered.
 *
 * <p>Specific implementations provide implementations for {@link #commitOrClean(Set)} and {@link
 * #abortFileGroup(Object)}
 *
 * @param <T> abstract type of file group
 */
abstract class BaseCommitService<T> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BaseCommitService.class);

  private final Table table;
  private final ExecutorService committerService;
  private final ConcurrentLinkedQueue<T> completedRewrites;
  private final ConcurrentLinkedQueue<String> inProgressCommits;
  private final List<T> committedRewrites;
  private final int rewritesPerCommit;
  private final AtomicBoolean running = new AtomicBoolean(false);

  /**
   * Constructs a {@link BaseCommitService}
   *
   * @param table table to perform commit on
   * @param rewritesPerCommit number of file groups to include in a commit
   */
  BaseCommitService(Table table, int rewritesPerCommit) {
    this.table = table;
    LOG.info(
        "Creating commit service for table {} with {} groups per commit", table, rewritesPerCommit);
    this.rewritesPerCommit = rewritesPerCommit;

    committerService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("Committer-Service").build());

    completedRewrites = Queues.newConcurrentLinkedQueue();
    committedRewrites = Lists.newArrayList();
    inProgressCommits = Queues.newConcurrentLinkedQueue();
  }

  /**
   * Perform a commit operation on the table for the set of file groups, should cleanup failed file
   * groups.
   *
   * @param batch set of file groups
   */
  protected abstract void commitOrClean(Set<T> batch);

  /**
   * Clean up a specified file set by removing any files created for that operation, should not
   * throw any exceptions
   *
   * @param group group of files which are not yet committed
   */
  protected abstract void abortFileGroup(T group);

  /** Starts a single threaded executor service for handling file group commits. */
  public void start() {
    Preconditions.checkState(running.compareAndSet(false, true), "Commit service already started");
    LOG.info("Starting commit service for {}", table);
    committerService.execute(
        () -> {
          while (running.get() || completedRewrites.size() > 0 || inProgressCommits.size() > 0) {
            try {
              if (completedRewrites.size() == 0 && inProgressCommits.size() == 0) {
                // give other threads a chance to make progress
                Thread.sleep(100);
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException("Interrupted while processing commits", e);
            }

            // commit whatever is left once done with writing.
            if (!running.get() && completedRewrites.size() > 0) {
              commitReadyCommitGroups();
            }
          }
        });
  }

  /**
   * Places a file group in the queue and commits a batch of file groups if {@link
   * #rewritesPerCommit} number of file groups are present in the queue.
   *
   * @param group file group to eventually be committed
   */
  public void offer(T group) {
    LOG.debug("Offered to commit service: {}", group);
    Preconditions.checkState(
        running.get(), "Cannot add rewrites to a service which has already been closed");
    completedRewrites.add(group);
    commitReadyCommitGroups();
  }

  /** Returns all File groups which have been committed */
  public List<T> results() {
    Preconditions.checkState(
        committerService.isShutdown(),
        "Cannot get results from a service which has not been closed");
    return committedRewrites;
  }

  @Override
  public void close() {
    Preconditions.checkState(
        running.compareAndSet(true, false), "Cannot close already closed commit service");
    LOG.info("Closing commit service for {} waiting for all commits to finish", table);
    committerService.shutdown();

    boolean timeout = false;
    try {
      // All rewrites have completed and all new files have been created, we are now waiting for
      // the commit pool to finish doing its commits to Iceberg State. In the case of partial
      // progress this should have been occurring simultaneously with rewrites, if not there should
      // be only a single commit operation.
      if (!committerService.awaitTermination(120, TimeUnit.MINUTES)) {
        LOG.warn(
            "Commit operation did not complete within 120 minutes of the all files "
                + "being rewritten. This may mean that some changes were not successfully committed to the "
                + "table.");
        timeout = true;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "Cannot complete commit for rewrite, commit service interrupted", e);
    }

    if (!completedRewrites.isEmpty() && timeout) {
      LOG.error("Attempting to cleanup uncommitted file groups");
      completedRewrites.forEach(this::abortFileGroup);
    }

    Preconditions.checkArgument(
        !timeout && completedRewrites.isEmpty(),
        "Timeout occurred when waiting for commits to complete. "
            + "{} file groups committed. {} file groups remain uncommitted. "
            + "Retry this operation to attempt rewriting the failed groups.",
        committedRewrites.size(),
        completedRewrites.size());

    Preconditions.checkState(
        completedRewrites.isEmpty(),
        "File groups offered after service was closed, " + "they were not successfully committed.");
  }

  private void commitReadyCommitGroups() {
    Set<T> batch = null;
    if (canCreateCommitGroup()) {
      synchronized (completedRewrites) {
        if (canCreateCommitGroup()) {
          batch = Sets.newHashSetWithExpectedSize(rewritesPerCommit);
          for (int i = 0; i < rewritesPerCommit && !completedRewrites.isEmpty(); i++) {
            batch.add(completedRewrites.poll());
          }
        }
      }
    }

    if (batch != null) {
      String inProgressCommitToken = UUID.randomUUID().toString();
      inProgressCommits.add(inProgressCommitToken);
      try {
        commitOrClean(batch);
        committedRewrites.addAll(batch);
      } catch (Exception e) {
        LOG.error("Failure during rewrite commit process, partial progress enabled. Ignoring", e);
      }
      inProgressCommits.remove(inProgressCommitToken);
    }
  }

  private boolean canCreateCommitGroup() {
    // Either we have a full commit group, or we have completed writing and need to commit
    // what is left over
    boolean fullCommitGroup = completedRewrites.size() >= rewritesPerCommit;
    boolean writingComplete = !running.get() && completedRewrites.size() > 0;
    return fullCommitGroup || writingComplete;
  }
}
