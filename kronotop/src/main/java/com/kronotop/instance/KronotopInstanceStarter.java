/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.instance;


import com.kronotop.cluster.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
What Des-Cartes [sic] did was a good step. You have added much several ways, & especially in taking the colours of thin
plates into philosophical consideration. If I have seen further it is by standing on the shoulders of Giants.

-- Isaac Newton, Letter to Robert Hooke, 5 February 1675.
 */

/**
 * The KronotopInstanceStarter class is responsible for starting and shutting down a KronotopInstance.
 */
public class KronotopInstanceStarter {
    private static final Logger LOGGER = LoggerFactory.getLogger(KronotopInstanceStarter.class);

    private static void greeting(Member member) {
        LOGGER.info("pid: {} has been started", ProcessHandle.current().pid());
        LOGGER.info("Kronotop on {}/{} Java {}",
                System.getProperty("os.name"),
                System.getProperty("os.arch"),
                System.getProperty("java.version"));
        LOGGER.info("Listening client connections on {}", member.getExternalAddress());
    }

    public static void main(String[] args) {
        KronotopInstance kronotopInstance = new KronotopInstanceWithRESP();
        Thread shutdownHook = createShutdownHook(kronotopInstance);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        try {
            kronotopInstance.start();
            greeting(kronotopInstance.getMember());
        } catch (Exception e) {
            LOGGER.error("Failed to start Kronotop instance", e);
            System.exit(1);
        }
    }

    private static Thread createShutdownHook(KronotopInstance instance) {
        return new Thread(() -> {
            try {
                instance.shutdown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                instance.closeFoundationDBConnection();
                LOGGER.info("Quit!");
            }
        });
    }
}
