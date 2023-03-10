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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acroteq.kafka.connect.source.twitter.TweetStreamProcessor.TweetStreamProcessorFactory;
import com.acroteq.kafka.connect.source.twitter.TwitterApiFactory.TwitterApiBuilder;
import com.acroteq.kafka.connect.source.twitter.TwitterRuleService.TwitterRuleServiceBuilder;
import com.twitter.clientlib.api.TweetsApi;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.Tweet;
import java.util.List;
import java.util.function.Consumer;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

/** @author <a href="mailto:github@adamcc.ch">Adam Crowther</a> */
@ExtendWith(MockitoExtension.class)
class TwitterStreamerTest {

    private static final String KEYWORD = "keyword";

    @Mock private TwitterApiBuilder twitterApiBuilder;
    @Mock private TwitterApi twitterApi;
    @Mock private TweetsApi tweetsApi;


    @Mock private TweetStreamProcessorFactory tweetStreamProcessorFactory;
    @Mock private TweetStreamProcessor tweetStreamProcessor;

    @Mock private TwitterRuleServiceBuilder twitterRuleServiceBuilder;
    @Mock private TwitterRuleService twitterRuleService;

    @Mock private TwitterConfig twitterConfig;
    @Mock private Consumer<Tweet> tweetConsumer;
    @Mock private Password bearerToken;

    @BeforeEach
    public void setUp() {
        when(twitterConfig.getBearerToken()).thenReturn(bearerToken);
        when(twitterConfig.getFilterKeywords()).thenReturn(List.of(KEYWORD));
        when(tweetStreamProcessor.isRunning()).thenReturn(true);
    }

    @SuppressWarnings("resource")
    @Test
    public void testStart() {
        mockFactories(() -> {
            // when:
            final TwitterStreamer twitterStreamer = TwitterStreamer.factory()
                                                                   .config(twitterConfig)
                                                                   .consumer(tweetConsumer)
                                                                   .start();
            // then:
            assertThat(twitterStreamer.isRunning(), is(true));
            verify(twitterRuleService).deleteAllRules();
            verify(twitterRuleService).addKeywordFilterRules(List.of(KEYWORD));
            verify(tweetStreamProcessorFactory).start();
        });
    }

    @Test
    public void testStop() {
        mockFactories(() -> {
            // given:
            // after the processor is closed, it should return not running
            doAnswer(a -> {
                when(tweetStreamProcessor.isRunning()).thenReturn(false);
                return null;
            }).when(tweetStreamProcessor)
              .close();

            final TwitterStreamer twitterStreamer = TwitterStreamer.factory()
                                                                   .config(twitterConfig)
                                                                   .consumer(tweetConsumer)
                                                                   .start();
            // when:
            twitterStreamer.stop();
            // then:
            assertThat(twitterStreamer.isRunning(), is(false));
            verify(tweetStreamProcessor).close();
        });
    }


    @SuppressWarnings({ "ResultOfMethodCallIgnored", "unchecked" })
    private void mockFactories(final Runnable test) {
        try (final MockedStatic<TwitterApiFactory> twitterApiFactoryMockedStatic = mockStatic(TwitterApiFactory.class);
             final MockedStatic<TweetStreamProcessor> tweetStreamProcessorMockedStatic = //
                   mockStatic(TweetStreamProcessor.class);
             final MockedStatic<TwitterRuleService> twitterRuleServiceMockedStatic = //
                   mockStatic(TwitterRuleService.class)) {
            twitterApiFactoryMockedStatic.when(TwitterApiFactory::newTwitterApi)
                                         .thenReturn(twitterApiBuilder);
            when(twitterApiBuilder.bearerToken(any(Password.class))).thenReturn(twitterApiBuilder);
            when(twitterApiBuilder.build()).thenReturn(twitterApi);
            when(twitterApi.tweets()).thenReturn(tweetsApi);

            tweetStreamProcessorMockedStatic.when(TweetStreamProcessor::factory)
                                            .thenReturn(tweetStreamProcessorFactory);
            when(tweetStreamProcessorFactory.tweetsApi(tweetsApi)).thenReturn(tweetStreamProcessorFactory);
            when(tweetStreamProcessorFactory.config(twitterConfig)).thenReturn(tweetStreamProcessorFactory);
            when(tweetStreamProcessorFactory.consumer(any(Consumer.class))).thenReturn(tweetStreamProcessorFactory);
            when(tweetStreamProcessorFactory.start()).thenReturn(tweetStreamProcessor);

            twitterRuleServiceMockedStatic.when(TwitterRuleService::builder)
                                          .thenReturn(twitterRuleServiceBuilder);
            when(twitterRuleServiceBuilder.tweetsApi(tweetsApi)).thenReturn(twitterRuleServiceBuilder);
            when(twitterRuleServiceBuilder.config(twitterConfig)).thenReturn(twitterRuleServiceBuilder);
            when(twitterRuleServiceBuilder.build()).thenReturn(twitterRuleService);

            test.run();
        }
    }
}