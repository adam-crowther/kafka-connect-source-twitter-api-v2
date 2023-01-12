package com.acroteq.kafka.connect.source;

import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.KAFKA_MAX_BATCH_INTERVAL_MS_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.KAFKA_MAX_BATCH_SIZE_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.KAFKA_TWEETS_TOPIC_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_BEARER_TOKEN_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_FILTER_KEYWORDS_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_RETRIES_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_TWEET_FIELDS_CONF;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TwitterV2SourceConnectorTest {

    private static final String TOPIC = "topic";
    private static final String PASSWORD = "password";
    private static final String KEYWORD = "keyword";
    private static final String FIELD = "field";
    private static final int RETRIES = 10;
    private static final int BATCH_SIZE = 50;
    private static final int MAX_TASKS = 1;

    @Test
    public void testVersion() {
        // given:
        final TwitterV2SourceConnector twitterV2SourceConnector = new TwitterV2SourceConnector();
        // when:
        final String version = twitterV2SourceConnector.version();
        // then:
        assertThat(version, is("0.0.0.0"));
    }

    @Test
    public void testStart() {
        // given:
        final TwitterV2SourceConnector twitterV2SourceConnector = new TwitterV2SourceConnector();
        final Map<String, String> settingsMap = createSettingsMap();
        // when:
        twitterV2SourceConnector.start(settingsMap);
        // then:
        assertThat(twitterV2SourceConnector.isRunning(), is(true));
        final List<Map<String, String>> taskConfigs = twitterV2SourceConnector.taskConfigs(MAX_TASKS);
        assertThat(taskConfigs.size(), is(1));
        assertThat(taskConfigs.get(0), is(settingsMap));
    }

    @Test
    public void testGetTaskConfigs_running() {
        // given:
        final TwitterV2SourceConnector twitterV2SourceConnector = new TwitterV2SourceConnector();
        final Map<String, String> settingsMap = createSettingsMap();
        twitterV2SourceConnector.start(settingsMap);
        // when:
        final List<Map<String, String>> taskConfigs = twitterV2SourceConnector.taskConfigs(MAX_TASKS);
        // then:
        assertThat(taskConfigs.size(), is(1));
        assertThat(taskConfigs.get(0), is(settingsMap));
    }

    @Test
    public void testGetTaskConfigs_notRunning() {
        // given:
        final TwitterV2SourceConnector twitterV2SourceConnector = new TwitterV2SourceConnector();
        // when:
        assertThrows(IllegalStateException.class, () -> twitterV2SourceConnector.taskConfigs(MAX_TASKS));
    }

    @Test
    public void testStop() {
        // given:
        final TwitterV2SourceConnector twitterV2SourceConnector = new TwitterV2SourceConnector();
        final Map<String, String> settingsMap = createSettingsMap();
        twitterV2SourceConnector.start(settingsMap);
        // when:
        twitterV2SourceConnector.stop();
        // then:
        assertThat(twitterV2SourceConnector.isRunning(), is(false));
    }

    @Test
    public void testTaskClass() {
        // given:
        final TwitterV2SourceConnector twitterV2SourceConnector = new TwitterV2SourceConnector();
        // when:
        final Class<? extends Task> taskClass = twitterV2SourceConnector.taskClass();
        // then:
        assertEquals(taskClass, TwitterV2SourceTask.class);
    }

    @Test
    public void testConfig() {
        // given:
        final TwitterV2SourceConnector twitterV2SourceConnector = new TwitterV2SourceConnector();
        final Map<String, String> settingsMap = createSettingsMap();
        twitterV2SourceConnector.start(settingsMap);
        // when:
        final ConfigDef configDef = twitterV2SourceConnector.config();
        // then:
        assertThat(configDef.names(),
                   containsInAnyOrder(TWITTER_BEARER_TOKEN_CONF,
                                      TWITTER_FILTER_KEYWORDS_CONF,
                                      TWITTER_TWEET_FIELDS_CONF,
                                      KAFKA_TWEETS_TOPIC_CONF,
                                      KAFKA_MAX_BATCH_SIZE_CONF,
                                      KAFKA_MAX_BATCH_INTERVAL_MS_CONF,
                                      TWITTER_RETRIES_CONF));
    }

    @NotNull
    private static Map<String, String> createSettingsMap() {
        return Map.of(KAFKA_TWEETS_TOPIC_CONF,
                      TOPIC,
                      TWITTER_FILTER_KEYWORDS_CONF,
                      KEYWORD,
                      TWITTER_TWEET_FIELDS_CONF,
                      FIELD,
                      TWITTER_BEARER_TOKEN_CONF,
                      PASSWORD,
                      KAFKA_MAX_BATCH_SIZE_CONF,
                      Integer.toString(BATCH_SIZE),
                      TWITTER_RETRIES_CONF,
                      Integer.toString(RETRIES));
    }
}