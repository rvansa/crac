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
import jdk.test.lib.containers.docker.DockerfileConfig;
import jdk.test.lib.crac.CracBuilder;
import jdk.test.lib.crac.CracTest;
import jdk.test.lib.crac.CracTestArg;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static jdk.test.lib.Asserts.*;

/**
 * @test TimerPolicyTest
 * @requires (os.family == "linux")
 * @requires docker.support
 * @library /test/lib
 * @build TimerPolicyTest
 * @run driver jdk.test.lib.crac.CracTest DEFAULT
 * @run driver jdk.test.lib.crac.CracTest CANCEL
 * @run driver jdk.test.lib.crac.CracTest SKIP_TIME
 * @run driver jdk.test.lib.crac.CracTest SKIP_EXECUTIONS
 */
public class TimerPolicyTest implements CracTest {

    private static final String imageName = Common.imageName("ju-timer-policy");

    @CracTestArg
    String policy;

    @Override
    public void test() throws Exception {
        if (!DockerTestUtils.canTestDocker()) {
            return;
        }
        CracBuilder builder = new CracBuilder().inDockerImage(imageName)
                .withBaseImage("ghcr.io/crac/test-base", "latest");
        try {
            builder.ensureContainerStarted();
            // Note: one limitation of libfaketime is that it doesn't work well with futex library that's
            // used in Object.wait(). A maybe-extra notify in afterRestore fixes that, though.
            builder.startCheckpoint(Container.ENGINE_COMMAND, "exec",
                            "-e", "LD_PRELOAD=/usr/lib/x86_64-linux-gnu/faketime/libfaketimeMT.so.1",
                            "-e", "FAKETIME_TIMESTAMP_FILE=/tmp/faketime",
                            "-e", "FAKETIME_NO_CACHE=1",
                            CracBuilder.CONTAINER_NAME, CracBuilder.DOCKER_JAVA)
                    .waitForCheckpointed();
            DockerTestUtils.execute(Container.ENGINE_COMMAND, "exec", CracBuilder.CONTAINER_NAME,
                    "bash", "-c", "echo '+1h' > /tmp/faketime").shouldHaveExitValue(0);
            builder.javaOption("jdk.crac.timer-policy", policy)
                    .startRestore(Container.ENGINE_COMMAND, "exec",
                            CracBuilder.CONTAINER_NAME, CracBuilder.DOCKER_JAVA)
                    .waitForSuccess();
        } finally {
            builder.ensureContainerKilled();
        }
    }

    @Override
    public void exec() throws Exception {
        Timer timer = new Timer(true);

        AtomicInteger nonPeriodic = new AtomicInteger();
        AtomicInteger fixedRate = new AtomicInteger();
        AtomicInteger fixedDelay = new AtomicInteger();
        timer.schedule(wrap(nonPeriodic::incrementAndGet), 60_000);
        timer.scheduleAtFixedRate(wrap(fixedRate::incrementAndGet), 60_000, 60_000);
        timer.schedule(wrap(fixedDelay::incrementAndGet), 60_000, 60_000);

        CountDownLatch fixedRateLatch = new CountDownLatch(1);
        CountDownLatch fixedDelayLatch = new CountDownLatch(1);
        timer.scheduleAtFixedRate(wrap(fixedRateLatch::countDown), 600_000, 50);
        timer.schedule(wrap(fixedDelayLatch::countDown), 600_000, 50);

        long before = System.currentTimeMillis();
        Core.checkpointRestore();
        long after = System.currentTimeMillis();
        assertGTE(after, before + 3_600_000);

        CountDownLatch latch = new CountDownLatch(1);
        timer.schedule(wrap(latch::countDown), 1);
        latch.await();

        switch (policy) {
            case "DEFAULT", "SKIP_EXECUTIONS" -> {
                // Make sure that these are not delayed forever
                fixedRateLatch.await();
                fixedDelayLatch.await();
            }
        }

        timer.cancel();

        switch (policy) {
            case "DEFAULT" -> {
                assertEquals(1, nonPeriodic.get());
                assertEquals(60, fixedRate.get());
                assertEquals(1, fixedDelay.get());
            }
            case "CANCEL" -> {
                assertEquals(0, nonPeriodic.get());
                assertEquals(0, fixedRate.get());
                assertEquals(0, fixedDelay.get());
            }
            case "SKIP_TIME" -> {
                assertEquals(0, nonPeriodic.get());
                assertEquals(0, fixedRate.get());
                assertEquals(0, fixedDelay.get());
            }
            case "SKIP_EXECUTIONS" -> {
                assertEquals(1, nonPeriodic.get());
                assertEquals(1, fixedRate.get());
                assertEquals(1, fixedDelay.get());
            }
        }
    }

    private TimerTask wrap(Runnable runnable) {
        return new TimerTask() {
            @Override
            public void run() {
                runnable.run();
            }
        };
    }
}
