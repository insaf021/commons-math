/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.math4.legacy.analysis.integration.gauss;

import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.math4.legacy.core.Pair;
import org.junit.Test;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;

/**
 * Test for {@link BaseRuleFactory}.
 *
 */
public class BaseRuleFactoryTest {
    /**
     * Tests that a given rule rule will be computed and added once to the cache
     * whatever the number of times this rule is called concurrently.
     */
    @Test
        public void testConcurrentCreation() throws InterruptedException,
                                                    ExecutionException {
        // Number of times the same rule will be called.
        final int numTasks = 20;

        final ThreadPoolExecutor exec
            = new ThreadPoolExecutor(3, numTasks, 1, TimeUnit.SECONDS,
                                     new ArrayBlockingQueue<>(2));

        final List<Future<Pair<double[], double[]>>> results
            = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            results.add(exec.submit(new RuleBuilder()));
        }

        // Ensure that all computations have completed.
        for (Future<Pair<double[], double[]>> f : results) {
            f.get();
        }
        exec.shutdown();

        // Assertion would fail if "getRuleInternal" were not "synchronized".
        final int n = RuleBuilder.getNumberOfCalls();
        Assert.assertEquals("Rule computation was called " + n + " times", 1, n);
    }

    /**
     * Tests that concurrent requests for rules of different orders on a shared
     * factory complete. Unsynchronized updates of the cache can corrupt it, in
     * which case a lookup spins indefinitely.
     */
    @Test
    public void testConcurrentGetRuleDifferentOrders() {
        // One thread per core, so that the threads run in parallel.
        final int numThreads = Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors()));
        final int numRules = 16 * numThreads;
        // A thread spinning in a corrupted map does not respond to interruption,
        // so it must not prevent the JVM from exiting.
        final ExecutorService exec = Executors.newFixedThreadPool(numThreads, r -> {
            final Thread thread = new Thread(r);
            thread.setDaemon(true);
            return thread;
        });
        try {
            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
                for (int round = 0; round < 1000; round++) {
                    final BaseRuleFactory<Double> factory = new TrivialRuleFactory();
                    // Compute the rules up front, so that the threads only race
                    // on the cache of double-precision rules.
                    for (int i = 1; i <= numRules; i++) {
                        factory.getRuleInternal(i);
                    }
                    final AtomicInteger started = new AtomicInteger();
                    final List<Future<?>> results = new ArrayList<>();
                    for (int t = 0; t < numThreads; t++) {
                        final int first = t + 1;
                        results.add(exec.submit(() -> {
                            // Wait for all threads, so that they are released together.
                            started.incrementAndGet();
                            while (started.get() < numThreads) {
                                Thread.yield();
                            }
                            for (int i = first; i <= numRules; i += numThreads) {
                                Assert.assertEquals(i, factory.getRule(i).getFirst().length);
                            }
                        }));
                    }
                    for (Future<?> f : results) {
                        f.get();
                    }
                }
            });
        } finally {
            exec.shutdownNow();
        }
    }

    /** Factory with a cheap rule computation. */
    private static final class TrivialRuleFactory extends BaseRuleFactory<Double> {
        @Override
        protected Pair<Double[], Double[]> computeRule(int numberOfPoints) {
            final Double[] p = new Double[numberOfPoints];
            final Double[] w = new Double[numberOfPoints];
            Arrays.fill(p, 0.0);
            Arrays.fill(w, 1.0);
            return new Pair<>(p, w);
        }
    }
}

class RuleBuilder implements Callable<Pair<double[], double[]>> {
    private static final DummyRuleFactory factory = new DummyRuleFactory();

    @Override
    public Pair<double[], double[]> call() {
        final int dummy = 2; // Always request the same rule.
        return factory.getRule(dummy);
    }

    public static int getNumberOfCalls() {
        return factory.getNumberOfCalls();
    }
}

class DummyRuleFactory extends BaseRuleFactory<Double> {
    /** Rule computations counter. */
    private static AtomicInteger nCalls = new AtomicInteger();

    @Override
    protected Pair<Double[], Double[]> computeRule(int order) {
        // Tracks whether this computation has been called more than once.
        nCalls.getAndIncrement();

        try {
            // Sleep to simulate computation time.
            Thread.sleep(20);
        } catch (InterruptedException e) {
            Assert.fail("Unexpected interruption");
        }

         // Dummy rule (but contents must exist).
        final Double[] p = new Double[order];
        final Double[] w = new Double[order];
        for (int i = 0; i < order; i++) {
            p[i] = Double.valueOf(i);
            w[i] = Double.valueOf(i);
        }
        return new Pair<>(p, w);
    }

    public int getNumberOfCalls() {
        return nCalls.get();
    }
}
