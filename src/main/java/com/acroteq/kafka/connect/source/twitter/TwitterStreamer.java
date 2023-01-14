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

import static com.acroteq.kafka.connect.source.twitter.TwitterApiFactory.newTwitterApi;

import com.twitter.clientlib.api.TweetsApi;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.Tweet;
import java.util.function.Consumer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.types.Password;

/** @author <a href="mailto:github@adamcc.ch">Adam Crowther</a> */
@Slf4j
public class TwitterStreamer {

    private final TweetStreamProcessor processor;

    public static TwitterStreamerFactory factory() {
        return new TwitterStreamerFactory();
    }

    private TwitterStreamer(@NonNull final TwitterConfig config, final Consumer<Tweet> consumer) {
        final Password bearerToken = config.getBearerToken();
        final TwitterApi apiInstance = newTwitterApi().bearerToken(bearerToken)
                                                      .build();
        final TweetsApi tweetsApi = apiInstance.tweets();

        final TwitterRuleService twitterRuleService = TwitterRuleService.builder()
                                                                        .tweetsApi(tweetsApi)
                                                                        .config(config)
                                                                        .build();
        twitterRuleService.deleteAllRules();
        twitterRuleService.addKeywordFilterRules(config.getFilterKeywords());

        processor = TweetStreamProcessor.factory()
                                        .tweetsApi(tweetsApi)
                                        .config(config)
                                        .consumer(consumer)
                                        .start();
    }

    public void stop() {
        processor.close();
    }

    public boolean isRunning() {
        return processor.isRunning();
    }

    public static class TwitterStreamerFactory {

        private TwitterConfig config;
        private Consumer<Tweet> tweetConsumer;

        public TwitterStreamerFactory config(final TwitterConfig config) {
            this.config = config;
            return this;
        }

        public TwitterStreamerFactory consumer(final Consumer<Tweet> tweetConsumer) {
            this.tweetConsumer = tweetConsumer;
            return this;
        }

        /** Start up the Twitter Streamer. */
        public TwitterStreamer start() {
            return new TwitterStreamer(config, tweetConsumer);
        }
    }
}
