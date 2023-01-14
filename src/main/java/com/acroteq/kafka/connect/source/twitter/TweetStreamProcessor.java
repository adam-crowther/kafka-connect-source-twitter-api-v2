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

import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.JSON;
import com.twitter.clientlib.api.TweetsApi;
import com.twitter.clientlib.api.TweetsApi.APIsearchStreamRequest;
import com.twitter.clientlib.model.FilteredStreamingTweetResponse;
import com.twitter.clientlib.model.Problem;
import com.twitter.clientlib.model.Tweet;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

/** @author <a href="mailto:github@adamcc.ch">Adam Crowther</a> */
@Slf4j
class TweetStreamProcessor implements AutoCloseable {

    private static final long SHUTDOWN_TIMEOUT_MS = 5000;

    private static final Gson GSON = JSON.getGson();

    private final TweetsApi tweetsApi;
    private final TwitterConfig config;
    private final ExecutorService streamExecutor;

    private BufferedReader tweetStreamReader;
    private final Consumer<Tweet> consumer;

    private final AtomicBoolean running = new AtomicBoolean(false);


    static TweetStreamProcessorFactory factory() {
        return new TweetStreamProcessorFactory();
    }

    /** Constructor. */
    private TweetStreamProcessor(final TweetsApi tweetsApi,
                                 final TwitterConfig config,
                                 final Consumer<Tweet> consumer) {
        this.tweetsApi = tweetsApi;
        this.config = config;
        this.consumer = consumer;

        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("twitter-thread-%d")
                                                                      .build();
        streamExecutor = newSingleThreadExecutor(threadFactory);
        executeSearch();
    }

    /** Execute tweetsApi.searchStream() and start a service executor thread that processes the incoming tweets. */
    private void executeSearch() {
        try {
            final APIsearchStreamRequest builder = tweetsApi.searchStream();
            Optional.of(config)
                    .map(TwitterConfig::getFields)
                    .filter(ObjectUtils::isNotEmpty)
                    .ifPresent(builder::tweetFields);

            log.info("Starting the tweet streamer.");
            final int retries = config.getRetries();
            final InputStream inputStream = builder.execute(retries);
            final InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            tweetStreamReader = new BufferedReader(inputStreamReader);
            streamExecutor.submit(this::processTweets);
        } catch (final ApiException e) {
            throw new TwitterException("Error while calling tweetApi.searchStream()", e);
        }
    }

    /** Process the incoming tweets that we get through the input stream. */
    private void processTweets() {
        try {
            running.set(true);
            tweetStreamReader.lines()
                             .filter(t -> isRunning())
                             .forEach(this::processTweet);
        } catch (final RuntimeException e) {
            if (running.get()) {
                log.error("Error while trying to process a tweet", e);
                close();
            }
        }
    }

    private void processTweet(final String json) {
        logTweetJson(json);
        final Optional<FilteredStreamingTweetResponse> tweet = Optional.of(json)
                                                                       .filter(StringUtils::isNotBlank)
                                                                       .map(this::convertJsonToResponse);

        tweet.ifPresent(this::checkResponseForErrors);
        tweet.map(FilteredStreamingTweetResponse::getData)
             .ifPresent(consumer);
    }

    private void logTweetJson(final String json) {
        if (isNotBlank(json)) {
            log.trace("Tweet stream processor received json: {}", json);
        } else {
            log.warn("Empty line received.");
        }
    }

    private void checkResponseForErrors(@NonNull final FilteredStreamingTweetResponse response) {
        final List<Problem> errors = Optional.of(response)
                                             .map(FilteredStreamingTweetResponse::getErrors)
                                             .orElse(emptyList());
        if (!errors.isEmpty()) {
            logErrorsAndThrowTwitterException(errors);
        }
    }

    private FilteredStreamingTweetResponse convertJsonToResponse(final String tweet) {
        return GSON.fromJson(tweet, FilteredStreamingTweetResponse.class);
    }

    private void logErrorsAndThrowTwitterException(@NonNull final List<Problem> errors) {
        final String errorSummary = errors.stream()
                                          .map(Problem::toString)
                                          .collect(joining(", "));

        errors.stream()
              .map(Problem::toString)
              .forEach(e -> log.warn("Received error response from tweetsApi.searchStream(): {}", e));

        throw new TwitterException("Error while processing tweet" + ":  \n" + errorSummary);
    }

    /**
     * Close the input stream and the reader, which has the effect of gracefully shutting down the stream of tweets from
     * Twitter.
     */
    @Override
    public void close() {
        running.set(false);
        closeTweetReader();
        shutDownExecutor();
    }

    private void closeTweetReader() {
        try {
            if (tweetStreamReader != null) {
                log.info("Closing the tweetStreamReader.");
                tweetStreamReader.close();
                tweetStreamReader = null;
            }
        } catch (final IOException e) {
            log.warn("Error while closing tweetStreamReader", e);
        }

    }

    private void shutDownExecutor() {
        try {
            if (!streamExecutor.isShutdown()) {
                log.info("Shutting down the tweet streamer.");

                streamExecutor.shutdown();
                final boolean terminated = streamExecutor.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                if (!terminated) {
                    log.warn("Twitter streamer did not terminate before timeout of {} ms.  Attempting to force it.",
                             SHUTDOWN_TIMEOUT_MS);
                    streamExecutor.shutdownNow();
                }

                log.info("Shut down successful.");
            } else {
                log.info("Tweet streamer is not running.");
            }
        } catch (final InterruptedException e) {
            log.warn("Interrupted while awaiting timeout of twitter streamer.");
        }
    }

    boolean isRunning() {
        return running.get() || !streamExecutor.isTerminated();
    }

    public static class TweetStreamProcessorFactory {

        private TweetsApi tweetsApi;

        private TwitterConfig config;

        private Consumer<Tweet> consumer;

        private TweetStreamProcessorFactory() {
        }

        TweetStreamProcessorFactory tweetsApi(final TweetsApi tweetsApi) {
            this.tweetsApi = tweetsApi;
            return this;
        }

        public TweetStreamProcessorFactory config(final TwitterConfig config) {
            this.config = config;
            return this;
        }

        TweetStreamProcessorFactory consumer(final Consumer<Tweet> consumer) {
            this.consumer = consumer;
            return this;
        }

        public TweetStreamProcessor start() {
            return new TweetStreamProcessor(tweetsApi, config, consumer);
        }
    }
}
