package com.acroteq.kafka.connect.source;

import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.KAFKA_MAX_BATCH_SIZE_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.KAFKA_TWEETS_TOPIC_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_BEARER_TOKEN_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_FILTER_KEYWORDS_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_RETRIES_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_TWEET_FIELDS_CONF;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acroteq.kafka.connect.source.twitter.TwitterConfig;
import com.acroteq.kafka.connect.source.twitter.TwitterStreamer;
import com.acroteq.kafka.connect.source.twitter.TwitterStreamer.TwitterStreamerFactory;
import com.twitter.clientlib.model.Tweet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.kafka.connect.source.SourceRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TwitterV2SourceTaskTest {

    private static final String TOPIC = "topic";
    private static final String PASSWORD = "password";
    private static final String KEYWORD = "keyword";
    private static final String FIELD = "field";
    private static final int RETRIES = 10;
    private static final int BATCH_SIZE = 50;
    private static final String TWEET_ID = "tweet-id";
    private static final String TWEET_TEXT = "tweet-text";


    @Mock private TwitterStreamerFactory twitterStreamerFactory;
    @Mock private TwitterStreamer twitterStreamer;

    @Captor private ArgumentCaptor<Consumer<Tweet>> tweetConsumerCaptor;


    private final TwitterV2SourceTask twitterV2SourceTask = new TwitterV2SourceTask();
    private final Map<String, String> settingsMap = createSettingsMap();
    private final Tweet tweet = new Tweet().id(TWEET_ID)
                                           .text(TWEET_TEXT);

    @Test
    public void testVersion() {
        // when:
        final String version = twitterV2SourceTask.version();
        // then:
        assertThat(version, is("0.0.0.0"));
    }

    @Test
    public void testStart() {
        mockTwitterStreamer(() -> {
            // when:
            twitterV2SourceTask.start(settingsMap);
            // then:
            verify(twitterStreamerFactory).start();
        });
    }

    @Test
    public void testStop_running() {
        mockTwitterStreamer(() -> {
            // given:
            twitterV2SourceTask.start(settingsMap);
            // when:
            twitterV2SourceTask.stop();
            // then:
            verify(twitterStreamerFactory).start();
            verify(twitterStreamer).stop();
        });
    }

    @Test
    public void testPoll_noTweets() {
        mockTwitterStreamer(() -> {
            // given:
            twitterV2SourceTask.start(settingsMap);
            // when:
            final List<SourceRecord> sourceRecords = twitterV2SourceTask.poll();
            // then:
            assertThat(sourceRecords, is(empty()));
        });
    }

    @Test
    public void testPoll_receivedTweets() {
        mockTwitterStreamer(() -> {
            // given:
            twitterV2SourceTask.start(settingsMap);
            verify(twitterStreamerFactory).consumer(tweetConsumerCaptor.capture());
            final Consumer<Tweet> tweetConsumer = tweetConsumerCaptor.getValue();
            // receive a tweet
            tweetConsumer.accept(tweet);

            // when:
            final List<SourceRecord> sourceRecords = twitterV2SourceTask.poll();
            // then:
            assertThat(sourceRecords, hasSize(1));
        });
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

    @SuppressWarnings({ "ResultOfMethodCallIgnored", "unchecked" })
    private void mockTwitterStreamer(final Runnable test) {
        try (final MockedStatic<TwitterStreamer> utilities = mockStatic(TwitterStreamer.class)) {
            utilities.when(TwitterStreamer::factory)
                     .thenReturn(twitterStreamerFactory);
            when(twitterStreamerFactory.config(any(TwitterConfig.class))).thenReturn(twitterStreamerFactory);
            when(twitterStreamerFactory.consumer(any(Consumer.class))).thenReturn(twitterStreamerFactory);
            when(twitterStreamerFactory.start()).thenReturn(twitterStreamer);
            lenient().when(twitterStreamer.isRunning())
                     .thenReturn(true);

            test.run();
        }
    }
}