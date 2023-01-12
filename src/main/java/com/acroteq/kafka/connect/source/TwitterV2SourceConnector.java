/*
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * For more information, please refer to <http://unlicense.org/>
 */
package com.acroteq.kafka.connect.source;

import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.createConfigDef;
import static com.acroteq.kafka.connect.source.util.Constants.VERSION;
import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

@Slf4j
public class TwitterV2SourceConnector extends SourceConnector {

    private TwitterV2SourceConnectorConfig config;

    private final AtomicBoolean running = new AtomicBoolean(false);

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(final Map<String, String> settingsMap) {
        if (running.compareAndSet(false, true)) {
            log.info("TwitterV2SourceConnector starting up.");
            config = new TwitterV2SourceConnectorConfig(settingsMap);
        } else {
            log.warn("TwitterV2SourceConnector already running.");
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TwitterV2SourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        if (running.get() && config != null) {
            checkArgument(maxTasks == 1, "MaxTasks must be 1.");

            final Map<String, String> taskSettings = config.originalsStrings();
            return List.of(taskSettings);
        } else {
            throw new IllegalStateException("The connector is not running.");
        }
    }

    @Override
    public void stop() {
        running.set(false);
        log.info("TwitterV2SourceConnector stopping.");
    }

    @Override
    public ConfigDef config() {
        return createConfigDef();
    }

    boolean isRunning() {
        return running.get();
    }
}
