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
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                        .create(TestingRescaleManagerContext.stableContext(), Instant.now());
        assertThat(testInstance.scalingIntervalMin).isEqualTo(scalingIntervalMin);
        assertThat(testInstance.scalingIntervalMax).isEqualTo(scalingIntervalMax);
        assertThat(testInstance.minParallelismChange).isEqualTo(minParallelismChange);
    }

    @Test
    void testInvalidConfiguration() {
        final Duration cooldownThreshold = Duration.ofMinutes(2);
        final TestingRescaleManagerContext ctx = TestingRescaleManagerContext.stableContext();
        assertThatThrownBy(
                        () ->
                                new DefaultRescaleManager(
                                        Instant.now(),
                                        ctx,
                                        cooldownThreshold,
                                        cooldownThreshold.minusNanos(1),
                                        1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDesiredChangeEventDuringCooldown() {
        final TestingRescaleManagerContext softScalePossibleCtx =
                TestingRescaleManagerContext.stableContext().withDesiredRescaling();
        final DefaultRescaleManager testInstance =
                softScalePossibleCtx.createTestInstanceInCooldownPhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(softScalePossibleCtx);

        softScalePossibleCtx.transitionIntoSoftScalingTimeframe();

        assertFinalStateWithRescale(softScalePossibleCtx);
    }

    @Test
    void testDesiredChangeEventInSoftRescalePhase() {
        final TestingRescaleManagerContext desiredRescalePossibleCtx =
                TestingRescaleManagerContext.stableContext().withDesiredRescaling();
        final DefaultRescaleManager testInstance =
                desiredRescalePossibleCtx.createTestInstanceInSoftRescalePhase();

        testInstance.onChange();

        assertFinalStateWithRescale(desiredRescalePossibleCtx);
    }

    @Test
    void testDesiredChangeEventInHardRescalePhase() {
        final TestingRescaleManagerContext desiredRescalePossibleCtx =
                TestingRescaleManagerContext.stableContext().withDesiredRescaling();
        final DefaultRescaleManager testInstance =
                desiredRescalePossibleCtx.createTestInstanceInHardRescalePhase();

        testInstance.onChange();

        assertFinalStateWithRescale(desiredRescalePossibleCtx);
    }

    @Test
    void testNoRescaleInCooldownPhase() {
        final TestingRescaleManagerContext noRescalePossibleCtx =
                TestingRescaleManagerContext.stableContext();
        final DefaultRescaleManager testInstance =
                noRescalePossibleCtx.createTestInstanceInCooldownPhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(noRescalePossibleCtx);

        noRescalePossibleCtx.transitionIntoSoftScalingTimeframe();

        assertIntermediateStateWithoutRescale(noRescalePossibleCtx);

        noRescalePossibleCtx.transitionIntoHardScalingTimeframe();

        assertThat(noRescalePossibleCtx.rescaleWasTriggered())
                .as("No rescaling should have happened even in the hard-rescaling phase.")
                .isFalse();
        assertThat(noRescalePossibleCtx.additionalTasksWaiting())
                .as("No further tasks should have been waiting for execution.")
                .isFalse();
    }

    @Test
    void testNoRescaleInSoftRescalePhase() {
        final TestingRescaleManagerContext noRescalePossibleCtx =
                TestingRescaleManagerContext.stableContext();
        final DefaultRescaleManager testInstance =
                noRescalePossibleCtx.createTestInstanceInSoftRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(noRescalePossibleCtx);

        noRescalePossibleCtx.transitionIntoHardScalingTimeframe();

        assertThat(noRescalePossibleCtx.rescaleWasTriggered())
                .as("No rescaling should have happened even in the hard-rescaling phase.")
                .isFalse();
        assertThat(noRescalePossibleCtx.additionalTasksWaiting())
                .as("No further tasks should have been waiting for execution.")
                .isFalse();
    }

    @Test
    void testNoResaleInHardRescalePhase() {
        final TestingRescaleManagerContext noRescalePossibleCtx =
                TestingRescaleManagerContext.stableContext();
        final DefaultRescaleManager testInstance =
                noRescalePossibleCtx.createTestInstanceInHardRescalePhase();

        testInstance.onChange();

        assertThat(noRescalePossibleCtx.rescaleWasTriggered())
                .as("No rescaling should have happened even in the hard-rescaling phase.")
                .isFalse();
        assertThat(noRescalePossibleCtx.additionalTasksWaiting())
                .as("No further tasks should have been waiting for execution.")
                .isFalse();
    }

    @Test
    void testSufficientChangeInCooldownPhase() {
        final TestingRescaleManagerContext hardRescalePossibleCtx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance =
                hardRescalePossibleCtx.createTestInstanceInCooldownPhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(hardRescalePossibleCtx);

        hardRescalePossibleCtx.transitionIntoSoftScalingTimeframe();

        assertIntermediateStateWithoutRescale(hardRescalePossibleCtx);

        hardRescalePossibleCtx.transitionIntoHardScalingTimeframe();

        assertFinalStateWithRescale(hardRescalePossibleCtx);
    }

    @Test
    void testSufficientChangeInSoftRescalePhase() {
        final TestingRescaleManagerContext hardRescalePossibleCtx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance =
                hardRescalePossibleCtx.createTestInstanceInSoftRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(hardRescalePossibleCtx);

        hardRescalePossibleCtx.transitionIntoHardScalingTimeframe();

        assertFinalStateWithRescale(hardRescalePossibleCtx);
    }

    @Test
    void testSufficientChangeInHardRescalePhase() {
        final TestingRescaleManagerContext hardRescalePossibleCtx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance =
                hardRescalePossibleCtx.createTestInstanceInHardRescalePhase();

        testInstance.onChange();

        assertFinalStateWithRescale(hardRescalePossibleCtx);
    }

    @Test
    void testSufficientChangeInCooldownWithSubsequentDesiredChangeInSoftRescalePhase() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInCooldownPhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        ctx.transitionIntoSoftScalingTimeframe();

        ctx.withDesiredRescaling();

        testInstance.onChange();

        assertThat(ctx.rescaleWasTriggered()).isTrue();
        assertThat(ctx.numberOfTasksWaiting())
                .as(
                        "There should be a task scheduled that allows transitioning into hard-rescaling phase.")
                .isEqualTo(1);
    }

    @Test
    void testSufficientChangeWithSubsequentDesiredChangeInSoftRescalePhase() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInSoftRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        assertThat(ctx.numberOfTasksWaiting())
                .as(
                        "There should be a task scheduled that allows transitioning into hard-rescaling phase.")
                .isEqualTo(1);

        ctx.withDesiredRescaling();

        testInstance.onChange();

        assertThat(ctx.rescaleWasTriggered()).isTrue();
    }

    @Test
    void
            testRevokedSufficientChangeInSoftRescalePhaseWithSubsequentSufficientChangeInHardRescalingPhase() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInSoftRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        assertThat(ctx.numberOfTasksWaiting())
                .as(
                        "There should be a task scheduled that allows transitioning into hard-rescaling phase.")
                .isEqualTo(1);

        ctx.revertAnyParallelismImprovements();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        assertThat(ctx.numberOfTasksWaiting())
                .as(
                        "There should be a task scheduled that allows transitioning into hard-rescaling phase.")
                .isEqualTo(1);

        ctx.transitionIntoHardScalingTimeframe();

        assertThat(ctx.rescaleWasTriggered())
                .as(
                        "No rescaling should have been triggered because of the previous revert of the additional resources.")
                .isFalse();
        assertThat(ctx.additionalTasksWaiting())
                .as(
                        "The transition to hard-rescaling should have happened without any additional tasks in waiting state.")
                .isFalse();

        ctx.withSufficientRescaling();

        testInstance.onChange();

        assertFinalStateWithRescale(ctx);
    }

    @Test
    void testRevokedChangeInHardRescalingPhaseCausesWithSubsequentSufficientChange() {
        final TestingRescaleManagerContext ctx = TestingRescaleManagerContext.stableContext();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInHardRescalePhase();

        testInstance.onChange();

        assertThat(ctx.rescaleWasTriggered()).isFalse();
        assertThat(ctx.additionalTasksWaiting()).isFalse();

        ctx.withSufficientRescaling();

        testInstance.onChange();

        assertFinalStateWithRescale(ctx);
    }

    private static void assertIntermediateStateWithoutRescale(TestingRescaleManagerContext ctx) {
        assertThat(ctx.rescaleWasTriggered())
                .as("The rescale should not have been triggered, yet.")
                .isFalse();
        assertThat(ctx.additionalTasksWaiting())
                .as("There should be still tasks being scheduled.")
                .isTrue();
    }

    private static void assertFinalStateWithRescale(TestingRescaleManagerContext ctx) {
        assertThat(ctx.rescaleWasTriggered())
                .as("The rescale should have been triggered already.")
                .isTrue();
        assertThat(ctx.additionalTasksWaiting())
                .as("All scheduled tasks should have been executed.")
                .isFalse();
    }

    /**
     * {@code TestingRescaleManagerContext} provides methods for adjusting the elapsed time and for
     * adjusting the available resources for rescaling.
     */
    private static class TestingRescaleManagerContext implements RescaleManager.Context {

        private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();

        // default configuration values to allow for easy transitioning between the phases
        private static final Duration SCALING_MIN = Duration.ofHours(1);
        private static final Duration SCALING_MAX = Duration.ofHours(2);
        private static final int MIN_PARALLELISM_CHANGE = 2;

        private static final int CURRENT_PARALLELISM = 1;

        // configuration that defines what kind of rescaling would be possible
        private final VertexParallelism currentVertexParallelism =
                createVertexParallelism(CURRENT_PARALLELISM);
        @Nullable private VertexParallelism availableVertexParallelism;

        // internal state used for assertions
        private final AtomicBoolean rescaleTriggered = new AtomicBoolean();
        private final SortedMap<Instant, List<Runnable>> scheduledTasks = new TreeMap<>();

        // time management - initializationTime can only be initialized once per context instance
        @Nullable private Instant initializationTime;
        private Duration elapsedTime = Duration.ZERO;

        private static VertexParallelism createVertexParallelism(int parallelism) {
            final Map<JobVertexID, Integer> map = new HashMap<>();
            map.put(JOB_VERTEX_ID, parallelism);

            return new VertexParallelism(map);
        }

        // ///////////////////////////////////////////////
        // Context creation
        // ///////////////////////////////////////////////

        public static TestingRescaleManagerContext stableContext() {
            return new TestingRescaleManagerContext();
        }

        private TestingRescaleManagerContext() {
            // no rescaling is enabled by default
            revertAnyParallelismImprovements();
        }

        public void revertAnyParallelismImprovements() {
            this.availableVertexParallelism = null;
        }

        public TestingRescaleManagerContext withDesiredRescaling() {
            this.availableVertexParallelism =
                    createVertexParallelism(CURRENT_PARALLELISM + MIN_PARALLELISM_CHANGE);

            return this;
        }

        public TestingRescaleManagerContext withSufficientRescaling() {
            this.availableVertexParallelism =
                    createVertexParallelism(CURRENT_PARALLELISM + MIN_PARALLELISM_CHANGE - 1);

            return this;
        }

        // ///////////////////////////////////////////////
        // RescaleManager.Context interface methods
        // ///////////////////////////////////////////////

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
            final Instant triggerTime =
                    Objects.requireNonNull(initializationTime).plus(elapsedTime).plus(delay);
            if (!scheduledTasks.containsKey(triggerTime)) {
                scheduledTasks.put(triggerTime, new ArrayList<>());
            }

            scheduledTasks.get(triggerTime).add(callback);
        }

        // ///////////////////////////////////////////////
        // Test instance creation
        // ///////////////////////////////////////////////

        /**
         * Creates the {@code DefaultRescaleManager} test instance and transitions into a period in
         * time where the instance is in cooldown phase.
         */
        public DefaultRescaleManager createTestInstanceInCooldownPhase() {
            return createTestInstance(this::transitionIntoCooldownTimeframe);
        }

        /**
         * Creates the {@code DefaultRescaleManager} test instance and transitions into a period in
         * time where the instance is in soft-rescaling phase.
         */
        public DefaultRescaleManager createTestInstanceInSoftRescalePhase() {
            return createTestInstance(this::transitionIntoSoftScalingTimeframe);
        }

        /**
         * Creates the {@code DefaultRescaleManager} test instance and transitions into a period in
         * time where the instance is in hard-rescaling phase.
         */
        public DefaultRescaleManager createTestInstanceInHardRescalePhase() {
            return createTestInstance(this::transitionIntoHardScalingTimeframe);
        }

        /**
         * Initializes the test instance and sets the context's elapsed time based on the passed
         * callback.
         */
        private DefaultRescaleManager createTestInstance(Runnable timeTransitioning) {
            Preconditions.checkState(
                    initializationTime == null, "Only one test instance should exist per context.");
            this.initializationTime = Instant.now();
            final DefaultRescaleManager testInstance =
                    new DefaultRescaleManager(
                            initializationTime,
                            // clock that returns the time based on the configured elapsedTime
                            () -> Objects.requireNonNull(initializationTime).plus(elapsedTime),
                            this,
                            SCALING_MIN,
                            SCALING_MAX,
                            MIN_PARALLELISM_CHANGE) {
                        @Override
                        public void onChange() {
                            super.onChange();

                            // hack to avoid calling this method in every test method
                            // we want to trigger tasks that are meant to run right-away
                            TestingRescaleManagerContext.this.triggerOutdatedTasks();
                        }
                    };

            timeTransitioning.run();
            return testInstance;
        }

        // ///////////////////////////////////////////////
        // Time-adjustment functionality
        // ///////////////////////////////////////////////

        /**
         * Transitions the context's time to a moment that falls into the test instance's cooldown
         * phase.
         */
        public void transitionIntoCooldownTimeframe() {
            this.elapsedTime = SCALING_MIN.dividedBy(2);
            this.triggerOutdatedTasks();
        }

        /**
         * Transitions the context's time to a moment that falls into the test instance's
         * soft-scaling phase.
         */
        public void transitionIntoSoftScalingTimeframe() {
            // the state transition is scheduled based on the current event's time rather than the
            // initializationTime
            this.elapsedTime = elapsedTime.plus(SCALING_MIN);

            // make sure that we're still below the scalingIntervalMax
            this.elapsedTime = elapsedTime.plus(SCALING_MAX.minus(elapsedTime).dividedBy(2));
            this.triggerOutdatedTasks();
        }

        /**
         * Transitions the context's time to a moment that falls into the test instance's
         * hard-scaling phase.
         */
        public void transitionIntoHardScalingTimeframe() {
            // the state transition is scheduled based on the current event's time rather than the
            // initializationTime
            this.elapsedTime = elapsedTime.plus(SCALING_MAX).plusMinutes(1);
            this.triggerOutdatedTasks();
        }

        private void triggerOutdatedTasks() {
            while (!scheduledTasks.isEmpty()) {
                final Instant timeOfExecution = scheduledTasks.firstKey();
                if (!timeOfExecution.isAfter(
                        Objects.requireNonNull(initializationTime).plus(elapsedTime))) {
                    scheduledTasks.remove(timeOfExecution).forEach(Runnable::run);
                } else {
                    break;
                }
            }
        }

        // ///////////////////////////////////////////////
        // Methods for verifying the context's state
        // ///////////////////////////////////////////////

        public boolean rescaleWasTriggered() {
            return rescaleTriggered.get();
        }

        public int numberOfTasksWaiting() {
            return scheduledTasks.size();
        }

        public boolean additionalTasksWaiting() {
            return !scheduledTasks.isEmpty();
        }
    }
}
