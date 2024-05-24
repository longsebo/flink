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

import org.apache.flink.runtime.scheduler.adaptive.scalingpolicy.EnforceMinimalIncreaseRescalingController;
import org.apache.flink.runtime.scheduler.adaptive.scalingpolicy.EnforceParallelismChangeRescalingController;
import org.apache.flink.runtime.scheduler.adaptive.scalingpolicy.RescalingController;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;

/**
 * {@code DefaultRescaleManager} manages the rescaling in depending on time of the previous rescale
 * operation and the available resources. It handles the event based on the following phases (in
 * that order):
 *
 * <ol>
 *   <li>Cooldown phase: No rescaling takes place (its upper threshold is defined by {@code
 *       scalingIntervalMin}.
 *   <li>Soft-rescaling phase: Rescaling is triggered if the desired amount of resources is
 *       available.
 *   <li>Hard-rescaling phase: Rescaling is triggered if a sufficient amount of resources is
 *       available (its lower threshold is defined by (@code scalingIntervalMax}).
 * </ol>
 *
 * @see Executing
 */
public class DefaultRescaleManager implements RescaleManager {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRescaleManager.class);

    private final Instant initializationTime;

    private final Duration scalingIntervalMin;
    @Nullable private final Duration scalingIntervalMax;

    private final int minParallelismChange;
    private final RescalingController softRescalingController;
    private final RescalingController hardRescalingController;

    private final RescaleManager.Context rescaleContext;

    private boolean rescaleScheduled = false;

    public DefaultRescaleManager(
            Instant initializationTime,
            RescaleManager.Context rescaleContext,
            Duration scalingIntervalMin,
            @Nullable Duration scalingIntervalMax,
            int minParallelismChange) {
        this.initializationTime = initializationTime;

        Preconditions.checkArgument(
                scalingIntervalMax == null || scalingIntervalMin.compareTo(scalingIntervalMax) <= 0,
                "scalingIntervalMax should at least match or be longer than scalingIntervalMin.");
        this.scalingIntervalMin = scalingIntervalMin;
        this.scalingIntervalMax = scalingIntervalMax;

        this.rescaleContext = rescaleContext;

        this.minParallelismChange = minParallelismChange;
        this.softRescalingController =
                new EnforceMinimalIncreaseRescalingController(minParallelismChange);
        this.hardRescalingController = new EnforceParallelismChangeRescalingController();
    }

    @Override
    public void onChange() {
        if (timeSinceLastRescale().compareTo(scalingIntervalMin) > 0) {
            maybeRescale();
        } else if (!rescaleScheduled) {
            rescaleScheduled = true;
            rescaleContext.scheduleOperation(this::maybeRescale, scalingIntervalMin);
        }
    }

    private Duration timeSinceLastRescale() {
        return Duration.between(this.initializationTime, Instant.now());
    }

    private void maybeRescale() {
        rescaleScheduled = false;
        if (shouldRescale(softRescalingController)) {
            LOG.info("Desired parallelism for job was reached: Rescaling will be triggered.");
            rescaleContext.rescale();
        } else if (scalingIntervalMax != null) {
            LOG.info(
                    "The longer the pipeline runs, the more the (small) resource gain is worth the restarting time. "
                            + "Last resource added does not meet the configured minimal parallelism change of {}. Forced rescaling will be triggered after {} if the resource is still there.",
                    minParallelismChange,
                    scalingIntervalMax);

            // reasoning for inconsistent scheduling:
            // https://lists.apache.org/thread/m2w2xzfjpxlw63j0k7tfxfgs0rshhwwr
            if (timeSinceLastRescale().compareTo(scalingIntervalMax) > 0) {
                rescaleEvenIfDesiredParallelismCannotBeMatched();
            } else {
                rescaleContext.scheduleOperation(
                        this::rescaleEvenIfDesiredParallelismCannotBeMatched, scalingIntervalMax);
            }
        }
    }

    private void rescaleEvenIfDesiredParallelismCannotBeMatched() {
        if (shouldRescale(hardRescalingController)) {
            LOG.info(
                    "Resources for desired job parallelism couldn't be collected after {}: Rescaling will be enforced.",
                    scalingIntervalMax);
            rescaleContext.rescale();
        }
    }

    private boolean shouldRescale(RescalingController rescalingController) {
        return rescaleContext
                .getAvailableVertexParallelism()
                .filter(
                        availableVertexParallelism ->
                                rescalingController.shouldRescale(
                                        rescaleContext.getCurrentVertexParallelism(),
                                        availableVertexParallelism))
                .isPresent();
    }
}
