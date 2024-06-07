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

import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;

import java.time.Instant;

public class TestingRescaleManager implements RescaleManager {

    private final Runnable onChangeRunnable;

    private TestingRescaleManager(Runnable onChangeRunnable) {
        this.onChangeRunnable = onChangeRunnable;
    }

    @Override
    public void onChange() {
        this.onChangeRunnable.run();
    }

    public static class Factory implements RescaleManager.Factory {

        private final Runnable onChangeRunnable;

        public static TestingRescaleManager.Factory noOpFactory() {
            return new Factory(() -> {});
        }

        public Factory(Runnable onChangeRunnable) {
            this.onChangeRunnable = onChangeRunnable;
        }

        @Override
        public RescaleManager create(
                Context ignoredContext,
                VertexParallelism ignoredCurrentVertexParallelism,
                Instant ignoredLastRescale) {
            return new TestingRescaleManager(onChangeRunnable);
        }
    }
}
