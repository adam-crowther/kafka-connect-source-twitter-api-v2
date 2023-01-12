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

import static com.acroteq.kafka.connect.source.converter.TweetConverter.TWEET_SCHEMA;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import com.acroteq.kafka.connect.source.converter.TweetConverter;
import com.acroteq.kafka.connect.source.twitter.TwitterConfig;
import com.acroteq.kafka.connect.source.twitter.TwitterStreamer;
import com.acroteq.kafka.connect.source.util.Constants;
import com.twitter.clientlib.model.Tweet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

@Slf4j
public class TwitterV2SourceTask extends SourceTask {

    private TwitterStreamer twitterStreamer;
    private String topic;
    private SourceRecordQueue sourceRecordQueue;
    private final TweetConverter tweetConverter = new TweetConverter();

    public TwitterV2SourceTask() {
        log.info("TwitterV2SourceTask CONSTRUCTOR.");
    }

    @Override
    public String version() {
        return Constants.VERSION;
    }

    @Override
    public void start(final Map<String, String> settingsMap) {
        log.info("TwitterV2SourceTask starting up.");

        final TwitterV2SourceConnectorConfig config = new TwitterV2SourceConnectorConfig(settingsMap);

        topic = config.getTopic();
        final int maxBatchSize = config.getMaxBatchSize();
        final int maxBatchIntervalMs = config.getMaxBatchIntervalMs();
        sourceRecordQueue = new SourceRecordQueue(maxBatchSize, maxBatchIntervalMs);

        final TwitterConfig twitterConfig = TwitterConfig.fromSettingsMap(settingsMap);
        twitterStreamer = TwitterStreamer.factory()
                                         .config(twitterConfig)
                                         .consumer(this::tweetConsumer)
                                         .start();

        log.info("TwitterV2SourceTask running.");
    }

    @Override
    public List<SourceRecord> poll() {
        final List<SourceRecord> sourceRecords = Optional.ofNullable(sourceRecordQueue)
                                                         .map(SourceRecordQueue::getBatch)
                                                         .orElse(emptyList());

        log.info("TwitterV2SourceTask poll: returned {} records.", sourceRecords.size());

        return sourceRecords;
    }

    @Override
    public void stop() {
        log.info("TwitterV2SourceTask stopping.");

        Optional.ofNullable(twitterStreamer)
                .filter(TwitterStreamer::isRunning)
                .ifPresent(TwitterStreamer::stop);

        log.info("TwitterV2SourceTask stopped.");
    }

    private void tweetConsumer(final Tweet tweet) {
        log.info("TwitterV2SourceTask tweet consumer.");

        final Struct tweetStruct = tweetConverter.convert(tweet);

        final Map<String, ?> sourcePartition = emptyMap();
        final Map<String, ?> sourceOffset = emptyMap();

        final SourceRecord sourceRecord = new SourceRecord(sourcePartition,
                                                           sourceOffset,
                                                           topic,
                                                           STRING_SCHEMA,
                                                           tweet.getConversationId(),
                                                           TWEET_SCHEMA,
                                                           tweetStruct);

        sourceRecordQueue.add(sourceRecord);
    }
}
