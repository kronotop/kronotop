/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop;

import com.kronotop.internal.FoundationDBFactory;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

import java.util.concurrent.atomic.AtomicBoolean;

// See https://stackoverflow.com/questions/75290490/junit5-before-and-after-suite-method-invocation
public class BeforeAfterSuiteListener implements TestExecutionListener {
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean(false);

    @Override
    public void testPlanExecutionStarted(TestPlan testPlan) {
        // With reuseForks=true a single fork JVM runs many test plans back to back. The global
        // FoundationDB connection can be opened only once per JVM and cannot be reopened after it is
        // closed, so closing it at the end of every test plan would poison the next test class in the
        // same JVM. Register one shutdown hook instead, closing the connection exactly once when the
        // fork JVM exits.
        if (SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(FoundationDBFactory::closeDatabase));
        }
    }
}
