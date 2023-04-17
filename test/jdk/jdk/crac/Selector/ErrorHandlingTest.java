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

import jdk.test.lib.crac.CracBuilder;
import jdk.test.lib.crac.CracTest;

import javax.crac.CheckpointException;
import javax.crac.Core;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

import static jdk.test.lib.Asserts.assertTrue;
import static jdk.test.lib.Asserts.fail;

/**
 * @test ErrorHandlingTest
 * @library /test/lib
 * @build ErrorHandlingTest
 * @run driver/timeout=15 jdk.test.lib.crac.CracTest
 */
public class ErrorHandlingTest implements CracTest {
    @Override
    public void test() throws Exception {
        new CracBuilder().doCheckpoint();
    }

    @Override
    public void exec() throws Exception {
        Selector selector = Selector.open();

        SocketChannel channel = SelectorProvider.provider().openSocketChannel();
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_CONNECT);
        selector.select();

        assertThrowing(CheckpointException.class, Core::checkpointRestore);
        // Without the fix the test got stuck here
        assertThrowing(CheckpointException.class, Core::checkpointRestore);

        channel.close();
        Core.checkpointRestore();
    }

    private interface ThrowingRunnable {
        void run() throws Throwable;
    }

    private static void assertThrowing(Class<? extends Throwable> tcls, ThrowingRunnable r) {
        try {
            r.run();
            fail("Should have thrown " + tcls.getName());
        } catch (Throwable t) {
            assertTrue(tcls.isInstance(t));
        }
    }
}
