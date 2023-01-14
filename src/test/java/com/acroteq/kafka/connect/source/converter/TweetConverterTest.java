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
package com.acroteq.kafka.connect.source.converter;

import static com.twitter.clientlib.model.ReplySettings.EVERYONE;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_ATTACHMENTS;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_AUTHOR_ID;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_CONTEXT_ANNOTATIONS;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_CONVERSATION_ID;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_CREATED_AT;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_EDIT_CONTROLS;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_EDIT_HISTORY_TWEET_IDS;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_ENTITIES;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_GEO;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_ID;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_IN_REPLY_TO_USER_ID;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_LANG;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_NON_PUBLIC_METRICS;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_ORGANIC_METRICS;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_POSSIBLY_SENSITIVE;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_PROMOTED_METRICS;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_PUBLIC_METRICS;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_REFERENCED_TWEETS;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_REPLY_SETTINGS;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_SOURCE;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_TEXT;
import static com.twitter.clientlib.model.Tweet.SERIALIZED_NAME_WITHHELD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import com.twitter.clientlib.model.Tweet;
import org.apache.kafka.connect.data.Struct;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/** @author <a href="mailto:github@adamcc.ch">Adam Crowther</a> */
class TweetConverterTest {

    private final TweetTestDataGenerator testDataGenerator = new TweetTestDataGenerator();

    private final TweetConverter tweetConverter = new TweetConverter();

    @Test
    public void testConvertTweet_AllFields() {
        // given:
        final Tweet tweet = testDataGenerator.createTweet();
        // when:
        final Struct struct = tweetConverter.convert(tweet);
        // then:

        MatcherAssert.assertThat(struct.getString(SERIALIZED_NAME_ID), Matchers.is(TweetTestDataGenerator.TWEET_ID));
        TweetConverterAssertions.assertDate(struct, SERIALIZED_NAME_CREATED_AT, TweetTestDataGenerator.CREATED_AT);
        MatcherAssert.assertThat(struct.getString(SERIALIZED_NAME_TEXT),
                                 Matchers.is(TweetTestDataGenerator.TWEET_TEXT));
        MatcherAssert.assertThat(struct.getString(SERIALIZED_NAME_AUTHOR_ID),
                                 Matchers.is(TweetTestDataGenerator.AUTHOR_ID));
        MatcherAssert.assertThat(struct.getString(SERIALIZED_NAME_IN_REPLY_TO_USER_ID),
                                 Matchers.is(TweetTestDataGenerator.IN_REPLY_TO_USER_ID));
        TweetConverterAssertions.assertAttachments(struct.getStruct(SERIALIZED_NAME_ATTACHMENTS));
        TweetConverterAssertions.assertReferencedTweets(struct.getArray(SERIALIZED_NAME_REFERENCED_TWEETS));
        TweetConverterAssertions.assertContextAnnotations(struct.getArray(SERIALIZED_NAME_CONTEXT_ANNOTATIONS));
        TweetConverterAssertions.assertWithheld(struct.getStruct(SERIALIZED_NAME_WITHHELD));
        TweetConverterAssertions.assertGeo(struct.getStruct(SERIALIZED_NAME_GEO));
        TweetConverterAssertions.assertEntities(struct.getStruct(SERIALIZED_NAME_ENTITIES));
        TweetConverterAssertions.assertPublicMetrics(struct.getStruct(SERIALIZED_NAME_PUBLIC_METRICS));
        assertThat(struct.getBoolean(SERIALIZED_NAME_POSSIBLY_SENSITIVE), is(true));
        MatcherAssert.assertThat(struct.getString(SERIALIZED_NAME_LANG), Matchers.is(TweetTestDataGenerator.LANG));
        MatcherAssert.assertThat(struct.getString(SERIALIZED_NAME_SOURCE), Matchers.is(TweetTestDataGenerator.SOURCE));
        TweetConverterAssertions.assertNonPublicMetrics(struct.getStruct(SERIALIZED_NAME_NON_PUBLIC_METRICS));
        TweetConverterAssertions.assertPromotedMetrics(struct.getStruct(SERIALIZED_NAME_PROMOTED_METRICS));
        TweetConverterAssertions.assertOrganicMetrics(struct.getStruct(SERIALIZED_NAME_ORGANIC_METRICS));
        TweetConverterAssertions.assertEditControls(struct.getStruct(SERIALIZED_NAME_EDIT_CONTROLS));
        MatcherAssert.assertThat(struct.getString(SERIALIZED_NAME_CONVERSATION_ID),
                                 Matchers.is(TweetTestDataGenerator.CONVERSATION_ID));
        TweetConverterAssertions.assertEditHistoryTweetIds(struct.getArray(SERIALIZED_NAME_EDIT_HISTORY_TWEET_IDS));
        assertThat(struct.getString(SERIALIZED_NAME_REPLY_SETTINGS), is(EVERYONE.getValue()));
    }


    @Test
    public void testConvertTweet_MinimalFields() {
        // given:
        final Tweet tweet = new Tweet().id(TweetTestDataGenerator.TWEET_ID)
                                       .text(TweetTestDataGenerator.TWEET_TEXT);
        // when:
        final Struct struct = tweetConverter.convert(tweet);
        // then:
        MatcherAssert.assertThat(struct.getString(SERIALIZED_NAME_ID), Matchers.is(TweetTestDataGenerator.TWEET_ID));
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_CREATED_AT);
        MatcherAssert.assertThat(struct.getString(SERIALIZED_NAME_TEXT),
                                 Matchers.is(TweetTestDataGenerator.TWEET_TEXT));
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_AUTHOR_ID);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_IN_REPLY_TO_USER_ID);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_ATTACHMENTS);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_REFERENCED_TWEETS);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_CONTEXT_ANNOTATIONS);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_WITHHELD);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_GEO);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_ENTITIES);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_PUBLIC_METRICS);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_POSSIBLY_SENSITIVE);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_LANG);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_SOURCE);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_NON_PUBLIC_METRICS);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_PROMOTED_METRICS);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_PUBLIC_METRICS);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_PUBLIC_METRICS);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_PUBLIC_METRICS);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_ORGANIC_METRICS);
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_EDIT_CONTROLS);
        assertThat(struct.getArray(SERIALIZED_NAME_EDIT_HISTORY_TWEET_IDS), is(empty()));
        TweetConverterAssertions.assertNull(struct, SERIALIZED_NAME_REPLY_SETTINGS);
    }
}