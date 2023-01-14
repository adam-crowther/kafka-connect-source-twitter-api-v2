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

import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.KAFKA_MAX_BATCH_INTERVAL_MS_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.KAFKA_MAX_BATCH_INTERVAL_MS_DEFAULT;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.KAFKA_MAX_BATCH_SIZE_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.KAFKA_MAX_BATCH_SIZE_DEFAULT;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.KAFKA_TWEETS_TOPIC_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.KAFKA_TWEETS_TOPIC_DEFAULT;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_BEARER_TOKEN_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_FILTER_KEYWORDS_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_RETRIES_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_RETRIES_DEFAULT;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_TWEET_FIELDS_CONF;
import static com.google.common.collect.Maps.newHashMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

/** @author <a href="mailto:github@adamcc.ch">Adam Crowther</a> */
class TwitterV2SourceConnectorConfigTest {

    private static final String TOPIC = "topic";
    private static final String PASSWORD = "password";
    private static final String KEYWORD = "keyword";
    private static final String FIELD = "field";
    private static final int RETRIES = 2;
    private static final int MAX_BATCH_SIZE = 50;
    private static final int MAX_BATCH_INTERVAL_MS = 1000;

    @Test
    void testAllFields() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        // when:
        final TwitterV2SourceConnectorConfig config = new TwitterV2SourceConnectorConfig(settingsMap);
        // then:
        assertThat(config.getTopic(), is(TOPIC));
        assertThat(config.getFilterKeywords(), contains(KEYWORD));
        assertThat(config.getTweetFields(), contains(FIELD));
        assertThat(config.getBearerToken()
                         .value(), is(PASSWORD));
        assertThat(config.getMaxBatchSize(), is(MAX_BATCH_SIZE));
        assertThat(config.getMaxBatchIntervalMs(), is(MAX_BATCH_INTERVAL_MS));
        assertThat(config.getRetries(), is(RETRIES));
    }

    @Test
    void testTopicMissing() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.remove(KAFKA_TWEETS_TOPIC_CONF);
        // when:
        final TwitterV2SourceConnectorConfig config = new TwitterV2SourceConnectorConfig(settingsMap);
        // then:
        assertThat(config.getTopic(), is(KAFKA_TWEETS_TOPIC_DEFAULT));
        assertThat(config.getFilterKeywords(), contains(KEYWORD));
        assertThat(config.getTweetFields(), contains(FIELD));
        assertThat(config.getBearerToken()
                         .value(), is(PASSWORD));
        assertThat(config.getMaxBatchSize(), is(MAX_BATCH_SIZE));
        assertThat(config.getMaxBatchIntervalMs(), is(MAX_BATCH_INTERVAL_MS));
        assertThat(config.getRetries(), is(RETRIES));
    }

    @Test
    void testTopicEmpty() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.put(KAFKA_TWEETS_TOPIC_CONF, "");
        // when:
        final ConfigException configException =
              assertThrows(ConfigException.class, () -> new TwitterV2SourceConnectorConfig(settingsMap));
        // then:
        assertThat(configException.getMessage(),
                   is("Invalid value  for configuration kafka.tweetsTopic: String must be non-empty"));
    }

    @Test
    void testFilterKeywordsMissing() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.remove(TWITTER_FILTER_KEYWORDS_CONF);
        // when:
        final ConfigException configException =
              assertThrows(ConfigException.class, () -> new TwitterV2SourceConnectorConfig(settingsMap));
        // then:
        assertThat(configException.getMessage(),
                   is("Missing required configuration \"twitter.filterKeywords\" which has no default value."));
    }

    @Test
    void testFilterKeywordsEmpty() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.put(TWITTER_FILTER_KEYWORDS_CONF, "");
        // when:
        final ConfigException configException =
              assertThrows(ConfigException.class, () -> new TwitterV2SourceConnectorConfig(settingsMap));
        assertThat(configException.getMessage(),
                   is("Invalid value  for configuration twitter.filterKeywords: String must be non-empty"));
    }

    @Test
    void testFieldsMissing() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.remove(TWITTER_TWEET_FIELDS_CONF);
        // when:
        final ConfigException configException =
              assertThrows(ConfigException.class, () -> new TwitterV2SourceConnectorConfig(settingsMap));
        // then:
        assertThat(configException.getMessage(),
                   is("Missing required configuration \"twitter.tweetFields\" which has no default value."));
    }

    @Test
    void testFieldsEmpty() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.put(TWITTER_TWEET_FIELDS_CONF, "");
        // when:
        final ConfigException configException =
              assertThrows(ConfigException.class, () -> new TwitterV2SourceConnectorConfig(settingsMap));
        // then:
        assertThat(configException.getMessage(),
                   is("Invalid value  for configuration twitter.tweetFields: String must be non-empty"));
    }

    @Test
    void testBearerTokenMissing() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.remove(TWITTER_BEARER_TOKEN_CONF);
        // when:
        final ConfigException configException =
              assertThrows(ConfigException.class, () -> new TwitterV2SourceConnectorConfig(settingsMap));
        // then:
        assertThat(configException.getMessage(),
                   is("Missing required configuration \"twitter.bearerToken\" which has no default value."));
    }

    @Test
    void testRetriesMissing() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.remove(TWITTER_RETRIES_CONF);
        // when:
        final TwitterV2SourceConnectorConfig config = new TwitterV2SourceConnectorConfig(settingsMap);
        // then:
        assertThat(config.getTopic(), is(TOPIC));
        assertThat(config.getFilterKeywords(), contains(KEYWORD));
        assertThat(config.getTweetFields(), contains(FIELD));
        assertThat(config.getBearerToken()
                         .value(), is(PASSWORD));
        assertThat(config.getMaxBatchSize(), is(MAX_BATCH_SIZE));
        assertThat(config.getMaxBatchIntervalMs(), is(MAX_BATCH_INTERVAL_MS));
        assertThat(config.getRetries(), is(TWITTER_RETRIES_DEFAULT));
    }

    @Test
    void testRetriesEmpty() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.put(TWITTER_RETRIES_CONF, "");
        // when:
        final ConfigException configException =
              assertThrows(ConfigException.class, () -> new TwitterV2SourceConnectorConfig(settingsMap));
        // then:
        assertThat(configException.getMessage(),
                   is("Invalid value  for configuration twitter.retries: Not a number of type INT"));
    }

    @Test
    void testRetriesInvalid() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.put(TWITTER_RETRIES_CONF, "invalid");
        // when:
        final ConfigException configException =
              assertThrows(ConfigException.class, () -> new TwitterV2SourceConnectorConfig(settingsMap));
        // then:
        assertThat(configException.getMessage(),
                   is("Invalid value invalid for configuration twitter.retries: Not a number of type INT"));
    }

    @Test
    void testRetriesOutOfRange() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.put(TWITTER_RETRIES_CONF, "100");
        // when:
        final ConfigException configException =
              assertThrows(ConfigException.class, () -> new TwitterV2SourceConnectorConfig(settingsMap));
        // then:
        assertThat(configException.getMessage(),
                   is("Invalid value 100 for configuration twitter.retries: Value must be no more than 50"));
    }

    @Test
    void testMaxBatchSizeMissing() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.remove(KAFKA_MAX_BATCH_SIZE_CONF);
        // when:
        final TwitterV2SourceConnectorConfig config = new TwitterV2SourceConnectorConfig(settingsMap);
        // then:
        assertThat(config.getTopic(), is(TOPIC));
        assertThat(config.getFilterKeywords(), contains(KEYWORD));
        assertThat(config.getTweetFields(), contains(FIELD));
        assertThat(config.getBearerToken()
                         .value(), is(PASSWORD));
        assertThat(config.getMaxBatchSize(), is(KAFKA_MAX_BATCH_SIZE_DEFAULT));
        assertThat(config.getMaxBatchIntervalMs(), is(MAX_BATCH_INTERVAL_MS));
        assertThat(config.getRetries(), is(RETRIES));
    }

    @Test
    void testMaxBatchSizeEmpty() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.put(KAFKA_MAX_BATCH_SIZE_CONF, "");
        // when:
        final ConfigException configException =
              assertThrows(ConfigException.class, () -> new TwitterV2SourceConnectorConfig(settingsMap));
        // then:
        assertThat(configException.getMessage(),
                   is("Invalid value  for configuration kafka.batch.maxSize: Not a number of type INT"));
    }

    @Test
    void testMaxBatchSizeInvalid() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.put(KAFKA_MAX_BATCH_SIZE_CONF, "invalid");
        // when:
        final ConfigException configException =
              assertThrows(ConfigException.class, () -> new TwitterV2SourceConnectorConfig(settingsMap));
        // then:
        assertThat(configException.getMessage(),
                   is("Invalid value invalid for configuration kafka.batch.maxSize: Not a number of type INT"));
    }

    @Test
    void testMaxBatchSizeOutOfRange() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.put(KAFKA_MAX_BATCH_SIZE_CONF, "1100");
        // when:
        final ConfigException configException =
              assertThrows(ConfigException.class, () -> new TwitterV2SourceConnectorConfig(settingsMap));
        // then:
        assertThat(configException.getMessage(),
                   is("Invalid value 1100 for configuration kafka.batch.maxSize: Value must be no more than 1000"));

    }

    @Test
    void testMaxBatchIntervalMsMissing() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.remove(KAFKA_MAX_BATCH_INTERVAL_MS_CONF);
        // when:
        final TwitterV2SourceConnectorConfig config = new TwitterV2SourceConnectorConfig(settingsMap);
        // then:
        assertThat(config.getTopic(), is(TOPIC));
        assertThat(config.getFilterKeywords(), contains(KEYWORD));
        assertThat(config.getTweetFields(), contains(FIELD));
        assertThat(config.getBearerToken()
                         .value(), is(PASSWORD));
        assertThat(config.getMaxBatchSize(), is(MAX_BATCH_SIZE));
        assertThat(config.getMaxBatchIntervalMs(), is(KAFKA_MAX_BATCH_INTERVAL_MS_DEFAULT));
        assertThat(config.getRetries(), is(RETRIES));
    }

    @Test
    void testMaxBatchIntervalMsEmpty() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.remove(KAFKA_MAX_BATCH_INTERVAL_MS_CONF);
        settingsMap.put(KAFKA_MAX_BATCH_INTERVAL_MS_CONF, "");
        // when:
        final ConfigException configException =
              assertThrows(ConfigException.class, () -> new TwitterV2SourceConnectorConfig(settingsMap));
        // then:
        assertThat(configException.getMessage(),
                   is("Invalid value  for configuration kafka.batch.maxIntervalMs: Not a number of type INT"));
    }

    @Test
    void testMaxBatchIntervalMsInvalid() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.remove(KAFKA_MAX_BATCH_INTERVAL_MS_CONF);
        settingsMap.put(KAFKA_MAX_BATCH_INTERVAL_MS_CONF, "invalid");
        // when:
        final ConfigException configException =
              assertThrows(ConfigException.class, () -> new TwitterV2SourceConnectorConfig(settingsMap));
        // then:
        assertThat(configException.getMessage(),
                   is("Invalid value invalid for configuration kafka.batch.maxIntervalMs: Not a number of type INT"));
    }

    @Test
    void testMaxBatchIntervalMsOutOfRange() {
        // given:
        final Map<String, String> settingsMap = createSettingsMap();
        settingsMap.remove(KAFKA_MAX_BATCH_INTERVAL_MS_CONF);
        settingsMap.put(KAFKA_MAX_BATCH_INTERVAL_MS_CONF, "120000");
        // when:
        final ConfigException configException =
              assertThrows(ConfigException.class, () -> new TwitterV2SourceConnectorConfig(settingsMap));
        // then:
        assertThat(configException.getMessage(),
                   is("Invalid value 120000 for configuration kafka.batch.maxIntervalMs: Value must be no more than "
                            + "60000"));
    }

    @Test
    void testCreateConfigDef() {
        // when:
        final ConfigDef configDef = TwitterV2SourceConnectorConfig.createConfigDef();
        // then:
        assertThat(configDef.names(),
                   containsInAnyOrder(TWITTER_BEARER_TOKEN_CONF,
                                      TWITTER_FILTER_KEYWORDS_CONF,
                                      TWITTER_TWEET_FIELDS_CONF,
                                      TWITTER_RETRIES_CONF,
                                      KAFKA_TWEETS_TOPIC_CONF,
                                      KAFKA_MAX_BATCH_SIZE_CONF,
                                      KAFKA_MAX_BATCH_INTERVAL_MS_CONF));
    }

    @NotNull
    private static Map<String, String> createSettingsMap() {
        return newHashMap(Map.of(KAFKA_TWEETS_TOPIC_CONF,
                                 TOPIC,
                                 TWITTER_FILTER_KEYWORDS_CONF,
                                 KEYWORD,
                                 TWITTER_TWEET_FIELDS_CONF,
                                 FIELD,
                                 TWITTER_BEARER_TOKEN_CONF,
                                 PASSWORD,
                                 TWITTER_RETRIES_CONF,
                                 Integer.toString(RETRIES),
                                 KAFKA_MAX_BATCH_SIZE_CONF,
                                 Integer.toString(MAX_BATCH_SIZE),
                                 KAFKA_MAX_BATCH_INTERVAL_MS_CONF,
                                 Integer.toString(MAX_BATCH_INTERVAL_MS)));
    }
}