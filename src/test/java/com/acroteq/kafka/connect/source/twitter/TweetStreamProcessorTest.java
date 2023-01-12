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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.JSON;
import com.twitter.clientlib.api.TweetsApi;
import com.twitter.clientlib.model.Tweet;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@Slf4j
@ExtendWith(MockitoExtension.class)
class TweetStreamProcessorTest {

    private static final int PROCESSING_TIMEOUT_SECONDS = 60;
    private static final int POLL_INTERVAL_SECONDS = 2;
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 20;
    private static final String FIELD = "field";
    private static final int RETRIES = 10;

    @Mock private TweetsApi tweetsApi;
    @Mock private TweetsApi.APIsearchStreamRequest apiSearchStreamRequest;
    @Mock private TwitterConfig config;
    @Mock private ApiException apiException;

    private List<Tweet> tweetList;

    private InputStream tweetStream;

    @SuppressWarnings("InstantiationOfUtilityClass")
    @BeforeAll
    static void setUpGson() {
        new JSON();
    }

    @BeforeEach
    void setUp() {
        tweetList = synchronizedList(new ArrayList<>());
        when(tweetsApi.searchStream()).thenReturn(apiSearchStreamRequest);
        when(apiSearchStreamRequest.tweetFields(anySet())).thenReturn(apiSearchStreamRequest);

        when(config.getRetries()).thenReturn(RETRIES);
        when(config.getFields()).thenReturn(Set.of(FIELD));
    }

    @SuppressWarnings("resource")
    @SneakyThrows
    @Test
    void testProcessTweets_wellFormed() {
        // given:

        tweetStream = getClass().getResourceAsStream("/well-formed-tweets.txt");
        when(apiSearchStreamRequest.execute(RETRIES)).thenReturn(tweetStream);

        // when:
        final TweetStreamProcessor tweetStreamProcessor = TweetStreamProcessor.factory()
                                                                              .tweetsApi(tweetsApi)
                                                                              .config(config)
                                                                              .consumer(this::tweetConsumer)
                                                                              .start();

        // then:
        assertThat(tweetStreamProcessor.isRunning(), is(true));
        assertTweetCount(11);
        verify(apiSearchStreamRequest).execute(RETRIES);
        tweetStreamProcessor.close();
        waitForShutdown(tweetStreamProcessor);
        assertThat(tweetStreamProcessor.isRunning(), is(false));
    }

    @SuppressWarnings("resource")
    @SneakyThrows
    @Test
    void testProcessTweets_emptyLine() {
        // given:
        tweetStream = getClass().getResourceAsStream("/blank-tweet.txt");
        when(apiSearchStreamRequest.execute(RETRIES)).thenReturn(tweetStream);

        // when:
        final TweetStreamProcessor tweetStreamProcessor = TweetStreamProcessor.factory()
                                                                              .tweetsApi(tweetsApi)
                                                                              .config(config)
                                                                              .consumer(this::tweetConsumer)
                                                                              .start();
        // then:
        assertThat(tweetStreamProcessor.isRunning(), is(true));
        assertTweetCount(11);
        verify(apiSearchStreamRequest).execute(RETRIES);
        tweetStreamProcessor.close();
        waitForShutdown(tweetStreamProcessor);
        assertThat(tweetStreamProcessor.isRunning(), is(false));
    }

    @SuppressWarnings("resource")
    @SneakyThrows
    @Test
    void testProcessTweets_malformed() {
        // given:
        tweetStream = getClass().getResourceAsStream("/malformed-tweet.txt");
        when(apiSearchStreamRequest.execute(RETRIES)).thenReturn(tweetStream);

        // when:
        final TweetStreamProcessor tweetStreamProcessor = TweetStreamProcessor.factory()
                                                                              .tweetsApi(tweetsApi)
                                                                              .config(config)
                                                                              .consumer(this::tweetConsumer)
                                                                              .start();
        // then:
        // The 7th is the malformed tweet, where we abort
        assertThat(tweetStreamProcessor.isRunning(), is(true));
        assertTweetCount(6);
        verify(apiSearchStreamRequest).execute(RETRIES);
        tweetStreamProcessor.close();
        waitForShutdown(tweetStreamProcessor);
        assertThat(tweetStreamProcessor.isRunning(), is(false));
    }

    @SuppressWarnings("resource")
    @SneakyThrows
    @Test
    void testProcessTweets_error() {
        // given:
        tweetStream = getClass().getResourceAsStream("/error-tweet.txt");
        when(apiSearchStreamRequest.execute(RETRIES)).thenReturn(tweetStream);

        // when:
        final TweetStreamProcessor tweetStreamProcessor = TweetStreamProcessor.factory()
                                                                              .tweetsApi(tweetsApi)
                                                                              .config(config)
                                                                              .consumer(this::tweetConsumer)
                                                                              .start();
        // then:
        assertThat(tweetStreamProcessor.isRunning(), is(true));
        assertTweetCount(6);
        verify(apiSearchStreamRequest).execute(RETRIES);
        tweetStreamProcessor.close();
        waitForShutdown(tweetStreamProcessor);
        assertThat(tweetStreamProcessor.isRunning(), is(false));
    }

    @SuppressWarnings("resource")
    @SneakyThrows
    @Test
    void testProcessTweets_exception() {
        // given:
        final byte[] emptyByteArray = new byte[0];
        tweetStream = new ByteArrayInputStream(emptyByteArray);
        when(apiSearchStreamRequest.execute(RETRIES)).thenThrow(apiException);

        // when:
        final TwitterException twitterException = assertThrows(TwitterException.class,
                                                               () -> TweetStreamProcessor.factory()
                                                                                         .tweetsApi(tweetsApi)
                                                                                         .config(config)
                                                                                         .consumer(this::tweetConsumer)
                                                                                         .start());
        assertThat(twitterException.getMessage(), is("Error while calling tweetApi.searchStream()"));
        verify(apiSearchStreamRequest).execute(RETRIES);
    }

    private void tweetConsumer(final Tweet tweet) {
        log.info("Incoming tweet: {}", tweet.toJson());
        tweetList.add(tweet);
    }

    private void assertTweetCount(final int tweetCount) {
        await().atMost(PROCESSING_TIMEOUT_SECONDS, SECONDS)
               .pollInterval(POLL_INTERVAL_SECONDS, SECONDS)
               .until(() -> tweetList.size() >= tweetCount);
        assertThat(tweetList.size(), is(equalTo(tweetCount)));
    }

    private void waitForShutdown(final TweetStreamProcessor tweetStreamProcessor) {
        await().atMost(SHUTDOWN_TIMEOUT_SECONDS, SECONDS)
               .pollInterval(POLL_INTERVAL_SECONDS, SECONDS)
               .until(() -> !tweetStreamProcessor.isRunning());
    }
}