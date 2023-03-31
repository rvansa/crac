/*
 * Copyright (c) 2023, Azul Systems, Inc. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

import jdk.crac.*;
import jdk.test.lib.Container;
import jdk.test.lib.containers.docker.Common;
import jdk.test.lib.containers.docker.DockerTestUtils;
import jdk.test.lib.crac.CracBuilder;
import jdk.test.lib.crac.CracTest;
import jdk.test.lib.crac.CracTestArg;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static jdk.test.lib.Asserts.*;

/**
 * @test TimerPolicyTest
 * @requires (os.family == "linux")
 * @requires docker.support
 * @library /test/lib
 * @build TimerPolicyTest
 *
 * @run driver jdk.test.lib.crac.CracTest DEFAULT
 * @run driver jdk.test.lib.crac.CracTest CANCEL
 * @run driver jdk.test.lib.crac.CracTest SKIP_TIME
 * @run driver jdk.test.lib.crac.CracTest SKIP_EXECUTIONS
 */
public class TimerPolicyTest implements CracTest {
    private static final String imageName = Common.imageName("juc-timer-policy");

    @CracTestArg
    String policy;

    @Override
    public void test() throws Exception {
        if (!DockerTestUtils.canTestDocker()) {
            return;
        }
        CracBuilder builder = new CracBuilder().inDockerImage(imageName);
        try {
            builder.doCheckpoint();

            builder.javaOption("jdk.crac.timer-policy", policy)
                    .startRestore(Container.ENGINE_COMMAND, "exec", CracBuilder.CONTAINER_NAME,
                    "unshare", "--fork", "--time", "--monotonic", "3600",
                    CracBuilder.DOCKER_JAVA).waitForSuccess();
        } finally {
            builder.ensureContainerKilled();
        }
    }

    @Override
    public void exec() throws Exception {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

        AtomicInteger nonPeriodic = new AtomicInteger();
        AtomicInteger fixedRate = new AtomicInteger();
        AtomicInteger fixedDelay = new AtomicInteger();
        executor.schedule(nonPeriodic::incrementAndGet, 1, TimeUnit.MINUTES);
        executor.scheduleAtFixedRate(fixedRate::incrementAndGet, 1, 1, TimeUnit.MINUTES);
        executor.scheduleWithFixedDelay(fixedDelay::incrementAndGet, 1, 1, TimeUnit.MINUTES);

        CountDownLatch fixedRateLatch = new CountDownLatch(1);
        CountDownLatch fixedDelayLatch = new CountDownLatch(1);
        executor.scheduleAtFixedRate(fixedRateLatch::countDown, 600_000, 50, TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(fixedDelayLatch::countDown, 600_000, 50, TimeUnit.MILLISECONDS);

        Core.checkpointRestore();

        CountDownLatch latch = new CountDownLatch(1);
        executor.schedule(latch::countDown, 1, TimeUnit.MILLISECONDS);
        latch.await();

        switch (policy) {
            case "DEFAULT", "SKIP_EXECUTIONS" -> {
                // Make sure that these are not delayed forever
                fixedRateLatch.await();
                fixedDelayLatch.await();
            }
        }

        int queueLength = executor.getQueue().size();
        executor.shutdown();
        assertTrue(executor.awaitTermination(100, TimeUnit.SECONDS));

        switch (policy) {
            case "DEFAULT" -> {
                assertEquals(1, nonPeriodic.get());
                assertEquals(60, fixedRate.get());
                assertEquals(1, fixedDelay.get());
                assertEquals(4, queueLength);
            }
            case "CANCEL" -> {
                assertEquals(0, nonPeriodic.get());
                assertEquals(0, fixedRate.get());
                assertEquals(0, fixedDelay.get());
                assertEquals(0, queueLength);
            }
            case "SKIP_TIME" -> {
                assertEquals(0, nonPeriodic.get());
                assertEquals(0, fixedRate.get());
                assertEquals(0, fixedDelay.get());
                assertEquals(5, queueLength);
            }
            case "SKIP_EXECUTIONS" -> {
                assertEquals(1, nonPeriodic.get());
                assertEquals(1, fixedRate.get());
                assertEquals(1, fixedDelay.get());
                assertEquals(4, queueLength);
            }
        }
    }
}
