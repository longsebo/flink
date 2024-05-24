/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

// TODO: Add more tests
class DefaultRescaleManagerTest {

    @Test
    void testProperConfiguration() {
        final Duration scalingIntervalMin = Duration.ofMillis(1337);
        final Duration scalingIntervalMax = Duration.ofMillis(7331);
        final int minParallelismChange = 42;
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MIN, scalingIntervalMin);
        configuration.set(JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MAX, scalingIntervalMax);
        configuration.set(JobManagerOptions.MIN_PARALLELISM_INCREASE, minParallelismChange);

        final DefaultRescaleManager testInstance =
                DefaultRescaleManager.Factory.fromSettings(
                                AdaptiveScheduler.Settings.of(configuration))
                        .create(TestingRescaleManagerContext.noRescaleContext(), Instant.now());
        assertThat(testInstance.scalingIntervalMin).isEqualTo(scalingIntervalMin);
        assertThat(testInstance.scalingIntervalMax).isEqualTo(scalingIntervalMax);
        assertThat(testInstance.minParallelismChange).isEqualTo(minParallelismChange);
    }

    @Test
    void testDesiredChangeEventDuringCooldown() {
        final Duration cooldownThreshold = Duration.ofHours(1);

        final TestingRescaleManagerContext softScalePossibleCtx =
                TestingRescaleManagerContext.scaleUpContext();
        final DefaultRescaleManager testInstance =
                softScalePossibleCtx.createTestInstanceInCooldownPhase(cooldownThreshold);
        testInstance.onChange();

        assertThat(softScalePossibleCtx.rescaleWasTriggered()).isFalse();
        assertThat(softScalePossibleCtx.operationWasScheduled()).isTrue();
        assertThat(softScalePossibleCtx.getDelayOfMostRecentlyScheduledTask())
                .isEqualTo(cooldownThreshold);

        softScalePossibleCtx.triggerOldestQueuedScheduledTask();
        assertThat(softScalePossibleCtx.rescaleWasTriggered()).isTrue();
    }

    @Test
    void testDesiredChangeEventAfterCooldownAndBeforeForcedRescalePhase() {
        final TestingRescaleManagerContext desiredRescalePossibleCtx =
                TestingRescaleManagerContext.scaleUpContext();
        final DefaultRescaleManager testInstance =
                desiredRescalePossibleCtx.createTestInstanceInSoftRescalePhase();
        testInstance.onChange();

        assertThat(desiredRescalePossibleCtx.rescaleWasTriggered()).isTrue();
        assertThat(desiredRescalePossibleCtx.operationWasScheduled()).isFalse();
    }

    @Test
    void testNotifyNewResourcesAvailableWithCanScaleUpWithoutForceTransitionsToRestarting() {
        // tests the same as #testChangeEventAfterCooldownAndBeforeForcedRescalePhase
    }

    @Test
    void testNotifyNewResourcesAvailableWithCantScaleUpWithoutForceAndCantScaleUpWithForce() {
        final TestingRescaleManagerContext noRescalePossibleCtx =
                TestingRescaleManagerContext.noRescaleContext();
        final Duration hardRescalePhaseThreshold = Duration.ofHours(1);
        final DefaultRescaleManager testInstance =
                noRescalePossibleCtx.createTestInstanceInSoftRescalePhase(
                        hardRescalePhaseThreshold);
        testInstance.onChange();

        assertThat(noRescalePossibleCtx.rescaleWasTriggered()).isFalse();
        assertThat(noRescalePossibleCtx.operationWasScheduled()).isTrue();
        assertThat(noRescalePossibleCtx.getDelayOfMostRecentlyScheduledTask())
                .isEqualTo(hardRescalePhaseThreshold);

        noRescalePossibleCtx.triggerOldestQueuedScheduledTask();
        assertThat(noRescalePossibleCtx.rescaleWasTriggered()).isFalse();
    }

    @Test
    void testNoResaleInHardRescalePhase() {
        final TestingRescaleManagerContext noRescalePossibleCtx =
                TestingRescaleManagerContext.noRescaleContext();
        final DefaultRescaleManager testInstance =
                noRescalePossibleCtx.createTestInstanceInHardRescalePhase();
        testInstance.onChange();

        assertThat(noRescalePossibleCtx.rescaleWasTriggered()).isFalse();
        assertThat(noRescalePossibleCtx.operationWasScheduled()).isFalse();
    }

    @Test
    void
            testNotifyNewResourcesAvailableWithCantScaleUpWithoutForceAndCanScaleUpWithForceScheduled() {
        // covered by
        // testNotifyNewResourcesAvailableWithCantScaleUpWithoutForceAndCantScaleUpWithForce
    }

    @Test
    void
            testNotifyNewResourcesAvailableWithCantScaleUpWithoutForceAndCanScaleUpWithForceImmediate() {
        final TestingRescaleManagerContext hardRescalePossibleCtx =
                TestingRescaleManagerContext.forcedScaleUpContext();
        final DefaultRescaleManager testInstance =
                hardRescalePossibleCtx.createTestInstanceInHardRescalePhase();
        testInstance.onChange();

        assertThat(hardRescalePossibleCtx.rescaleWasTriggered()).isTrue();
        assertThat(hardRescalePossibleCtx.operationWasScheduled()).isFalse();
    }

    private static class TestingRescaleManagerContext implements RescaleManager.Context {

        private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();
        private static final int MIN_PARALLELISM_CHANGE = 2;

        private final VertexParallelism currentVertexParallelism;
        @Nullable private final VertexParallelism availableVertexParallelism;
        private final AtomicBoolean rescaleTriggered = new AtomicBoolean();
        private final AtomicReference<Duration> operationDelay = new AtomicReference<>();
        private final Queue<Runnable> scheduledTasks = new LinkedList<>();

        public static TestingRescaleManagerContext scaleUpContext() {
            final int currentParallelism = 1;
            return new TestingRescaleManagerContext(
                    createVertexParallelism(currentParallelism),
                    createVertexParallelism(currentParallelism + MIN_PARALLELISM_CHANGE));
        }

        public static TestingRescaleManagerContext forcedScaleUpContext() {
            final int currentParallelism = 1;
            return new TestingRescaleManagerContext(
                    createVertexParallelism(currentParallelism),
                    createVertexParallelism(currentParallelism + MIN_PARALLELISM_CHANGE - 1));
        }

        public static TestingRescaleManagerContext noRescaleContext() {
            final VertexParallelism vertexParallelism = createVertexParallelism(1);
            return new TestingRescaleManagerContext(vertexParallelism, vertexParallelism);
        }

        private TestingRescaleManagerContext(
                VertexParallelism currentVertexParallelism,
                @Nullable VertexParallelism availableVertexParallelism) {
            this.currentVertexParallelism = currentVertexParallelism;
            this.availableVertexParallelism = availableVertexParallelism;
        }

        @Override
        public VertexParallelism getCurrentVertexParallelism() {
            return this.currentVertexParallelism;
        }

        @Override
        public Optional<VertexParallelism> getAvailableVertexParallelism() {
            return Optional.ofNullable(availableVertexParallelism);
        }

        @Override
        public void rescale() {
            rescaleTriggered.set(true);
        }

        @Override
        public void scheduleOperation(Runnable callback, Duration delay) {
            operationDelay.set(delay);
            scheduledTasks.add(callback);
        }

        public DefaultRescaleManager createTestInstanceInCooldownPhase(
                Duration cooldownPhaseDuration) {
            return createTestInstance(cooldownPhaseDuration, cooldownPhaseDuration.plusHours(1));
        }

        public DefaultRescaleManager createTestInstanceInSoftRescalePhase() {
            return createTestInstanceInSoftRescalePhase(Duration.ofHours(2));
        }

        public DefaultRescaleManager createTestInstanceInSoftRescalePhase(
                Duration hardRescalePhaseThreshold) {
            return createTestInstance(Duration.ZERO, hardRescalePhaseThreshold);
        }

        public DefaultRescaleManager createTestInstanceInHardRescalePhase() {
            return createTestInstance(Duration.ZERO, Duration.ZERO);
        }

        public DefaultRescaleManager createTestInstance(
                Duration scalingIntervalMin, Duration scalingIntervalMax) {
            return new DefaultRescaleManager(
                    Instant.now(),
                    this,
                    scalingIntervalMin,
                    scalingIntervalMax,
                    MIN_PARALLELISM_CHANGE);
        }

        public boolean rescaleWasTriggered() {
            return rescaleTriggered.get();
        }

        public boolean operationWasScheduled() {
            return !scheduledTasks.isEmpty();
        }

        public Duration getDelayOfMostRecentlyScheduledTask() {
            return operationDelay.get();
        }

        public void triggerOldestQueuedScheduledTask() {
            Objects.requireNonNull(scheduledTasks.poll()).run();
        }

        private static VertexParallelism createVertexParallelism(int parallelism) {
            final Map<JobVertexID, Integer> map = new HashMap<>();
            map.put(JOB_VERTEX_ID, parallelism);

            return new VertexParallelism(map);
        }
    }
}
