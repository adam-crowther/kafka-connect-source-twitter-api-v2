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
package com.acroteq.kafka.connect.source.twitter;

import static java.util.Collections.synchronizedList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

import com.twitter.clientlib.JSON;
import com.twitter.clientlib.model.Tweet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class TwitterStreamerIntegrationTest {

    private static final int PROCESSING_TIMEOUT_SECONDS = 60;
    private static final int POLL_INTERVAL_SECONDS = 2;
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 20;

    private static final int TWEET_COUNT = 10;

    private static final String KEYWORD_JAVA = "java";
    private static final String KEYWORD_JAVASCRIPT = "javascript";
    private static final String KEYWORD_TYPESCRIPT = "typescript";
    private static final String KEYWORD_SCALA = "scala";
    private static final String KEYWORD_PYTHON = "python";
    private static final String KEYWORD_RUBY = "ruby";
    private static final String KEYWORD_KAFKA = "kafka";
    private static final String KEYWORD_DOCKER = "docker";
    private static final String KEYWORD_KUBERNETES = "kubernetes";
    private static final String KEYWORD_SPRINGBOOT = "springboot";
    private static final List<String> KEYWORD_LIST = List.of(KEYWORD_JAVA,
                                                             KEYWORD_JAVASCRIPT,
                                                             KEYWORD_TYPESCRIPT,
                                                             KEYWORD_SCALA,
                                                             KEYWORD_KAFKA,
                                                             KEYWORD_PYTHON,
                                                             KEYWORD_RUBY,
                                                             KEYWORD_DOCKER,
                                                             KEYWORD_KUBERNETES,
                                                             KEYWORD_SPRINGBOOT);

    private static final String FIELD_ID = "id";
    private static final String FIELD_TEXT = "text";
    private static final String FIELD_AUTHOR_ID = "author_id";
    private static final String FIELD_CREATED_AT = "created_at";
    private static final String FIELD_CONVERSATION_ID = "conversation_id";
    private static final String FIELD_LANG = "lang";
    private static final String FIELD_SOURCE = "source";
    private static final Set<String> FIELD_SET = Set.of(FIELD_ID,
                                                        FIELD_TEXT,
                                                        FIELD_AUTHOR_ID,
                                                        FIELD_CREATED_AT,
                                                        FIELD_CONVERSATION_ID,
                                                        FIELD_LANG,
                                                        FIELD_SOURCE);

    private static final Password BEARER_TOKEN =
          new Password("AAAAAAAAAAAAAAAAAAAAAFMglAEAAAAAaOI1R94G%2BENPw6ObRAm%2Bndd6jQs"
                             + "%3DzmHY8TU1CztUsVLWyNUgOxEoTXFFcGwyM6Bh1MI07yENoq7Op0");
    private List<Tweet> tweetList;
    private final TwitterConfig config = TwitterConfig.builder()
                                                      .bearerToken(BEARER_TOKEN)
                                                      .fields(FIELD_SET)
                                                      .filterKeywords(KEYWORD_LIST)
                                                      .retries(10)
                                                      .build();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeAll
    static void setUpGson() {
        JSON.getGson();
    }

    @BeforeEach
    void setUp() {
        tweetList = synchronizedList(new ArrayList<>());
    }

    @Test
    void testStart_ok() {
        // when:
        final TwitterStreamer twitterStreamer = TwitterStreamer.factory()
                                                               .config(config)
                                                               .consumer(this::tweetConsumer)
                                                               .start();
        // then:
        assertThat(twitterStreamer.isRunning(), is(true));
        assertTweetCount();
        twitterStreamer.stop();
        waitForShutdown(twitterStreamer::isRunning);
        assertThat(twitterStreamer.isRunning(), is(false));
    }

    private void tweetConsumer(final Tweet tweet) {
        log.info("Incoming tweet: {}", tweet.toJson());
        tweetList.add(tweet);
    }

    private void assertTweetCount() {
        await().atMost(PROCESSING_TIMEOUT_SECONDS, SECONDS)
               .pollInterval(POLL_INTERVAL_SECONDS, SECONDS)
               .until(() -> tweetList.size() >= TWEET_COUNT);
        assertThat(tweetList.size(), is(greaterThanOrEqualTo(TWEET_COUNT)));
    }

    private void waitForShutdown(final BooleanSupplier isRunning) {
        await().atMost(SHUTDOWN_TIMEOUT_SECONDS, SECONDS)
               .pollInterval(POLL_INTERVAL_SECONDS, SECONDS)
               .until(() -> !isRunning.getAsBoolean());
    }

}