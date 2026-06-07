/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.ctl;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
        name = "kronotop-ctl",
        description = "Control tool for Kronotop clusters.",
        subcommands = {BootstrapCommand.class},
        mixinStandardHelpOptions = true,
        versionProvider = VersionProvider.class
)
public class KronotopCTL implements Runnable {

    public static void main(String[] args) {
        configureLogging();
        int exitCode = new CommandLine(new KronotopCTL()).execute(args);
        System.exit(exitCode);
    }

    private static void configureLogging() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.reset();

        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        encoder.setPattern("%d{HH:mm:ss.SSS} %-5level - %msg%n");
        encoder.start();

        ConsoleAppender<ILoggingEvent> appender = new ConsoleAppender<>();
        appender.setContext(context);
        appender.setEncoder(encoder);
        appender.start();

        ch.qos.logback.classic.Logger rootLogger = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        rootLogger.detachAndStopAllAppenders();
        rootLogger.addAppender(appender);
        rootLogger.setLevel(ch.qos.logback.classic.Level.INFO);
    }

    @Override
    public void run() {
        new CommandLine(this).usage(System.out);
    }
}
