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

import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.ANNOTATION_END;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.ANNOTATION_NORMALIZED_TEXT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.ANNOTATION_PROBABILITY;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.ANNOTATION_START;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.ANNOTATION_TYPE;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.CASHTAG_END;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.CASHTAG_START;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.CASHTAG_TAG;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.CONTEXT_ANNOTATION_DOMAIN_DESCRIPTION;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.CONTEXT_ANNOTATION_DOMAIN_ID;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.CONTEXT_ANNOTATION_DOMAIN_NAME;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.CONTEXT_ANNOTATION_ENTITY_DESCRIPTION;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.CONTEXT_ANNOTATION_ENTITY_ID;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.CONTEXT_ANNOTATION_ENTITY_NAME;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.EDIT_CONTROLS_EDITABLE_UNTIL;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.EDIT_CONTROLS_EDITS_REMAINING;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.EDIT_CONTROLS_IS_EDIT_ELIGIBLE;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.EDIT_HISTORY_TWEET_ID;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.GEO_PLACE_ID;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.HASHTAG_END;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.HASHTAG_START;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.HASHTAG_TYPE;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.IMAGE_HEIGHT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.IMAGE_URL;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.IMAGE_WIDTH;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.MEDIA_KEY;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.MENTION_END;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.MENTION_ID;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.MENTION_START;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.MENTION_USERNAME;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.NON_PUBLIC_METRICS_IMPRESSION_COUNT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.ORGANIC_METRICS_IMPRESSION_COUNT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.ORGANIC_METRICS_LIKE_COUNT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.ORGANIC_METRICS_REPLY_COUNT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.ORGANIC_METRICS_RETWEET_COUNT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.POINT_COORDINATES_LAT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.POINT_COORDINATES_LONG;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.POLL_ID;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.PROMOTED_METRICS_IMPRESSION_COUNT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.PROMOTED_METRICS_LIKE_COUNT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.PROMOTED_METRICS_REPLY_COUNT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.PROMOTED_METRICS_RETWEET_COUNT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.PUBLIC_METRICS_LIKE_COUNT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.PUBLIC_METRICS_QUOTE_COUNT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.PUBLIC_METRICS_REPLY_COUNT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.PUBLIC_METRICS_RETWEET_COUNT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.REFERENCED_TWEET_ID;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.URL_DESCRIPTION;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.URL_END;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.URL_MEDIA_KEY;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.URL_START;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.URL_STATUS;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.URL_TITLE;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.URL_URL;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.WITHHELD_COUNTRY_CODE;
import static com.twitter.clientlib.model.ContextAnnotation.SERIALIZED_NAME_DOMAIN;
import static com.twitter.clientlib.model.ContextAnnotation.SERIALIZED_NAME_ENTITY;
import static com.twitter.clientlib.model.FullTextEntities.SERIALIZED_NAME_ANNOTATIONS;
import static com.twitter.clientlib.model.FullTextEntities.SERIALIZED_NAME_CASHTAGS;
import static com.twitter.clientlib.model.FullTextEntities.SERIALIZED_NAME_HASHTAGS;
import static com.twitter.clientlib.model.FullTextEntities.SERIALIZED_NAME_MENTIONS;
import static com.twitter.clientlib.model.FullTextEntities.SERIALIZED_NAME_URLS;
import static com.twitter.clientlib.model.FullTextEntitiesAnnotations.SERIALIZED_NAME_NORMALIZED_TEXT;
import static com.twitter.clientlib.model.FullTextEntitiesAnnotations.SERIALIZED_NAME_PROBABILITY;
import static com.twitter.clientlib.model.MentionEntity.SERIALIZED_NAME_ID;
import static com.twitter.clientlib.model.MentionEntity.SERIALIZED_NAME_USERNAME;
import static com.twitter.clientlib.model.Point.TypeEnum.POINT;
import static com.twitter.clientlib.model.TweetAttachments.SERIALIZED_NAME_MEDIA_KEYS;
import static com.twitter.clientlib.model.TweetAttachments.SERIALIZED_NAME_POLL_IDS;
import static com.twitter.clientlib.model.TweetGeo.SERIALIZED_NAME_PLACE_ID;
import static com.twitter.clientlib.model.TweetReferencedTweets.TypeEnum.QUOTED;
import static com.twitter.clientlib.model.TweetWithheld.SERIALIZED_NAME_COPYRIGHT;
import static com.twitter.clientlib.model.TweetWithheld.SERIALIZED_NAME_COUNTRY_CODES;
import static com.twitter.clientlib.model.TweetWithheld.SERIALIZED_NAME_SCOPE;
import static com.twitter.clientlib.model.TweetWithheld.ScopeEnum.TWEET;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_DESCRIPTION;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_DISPLAY_URL;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_EXPANDED_URL;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_MEDIA_KEY;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_STATUS;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_TITLE;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_UNWOUND_URL;
import static com.twitter.clientlib.model.UrlImage.SERIALIZED_NAME_HEIGHT;
import static com.twitter.clientlib.model.UrlImage.SERIALIZED_NAME_WIDTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.twitter.clientlib.model.CashtagEntity;
import com.twitter.clientlib.model.ContextAnnotationDomainFields;
import com.twitter.clientlib.model.ContextAnnotationEntityFields;
import com.twitter.clientlib.model.FullTextEntitiesAnnotations;
import com.twitter.clientlib.model.HashtagEntity;
import com.twitter.clientlib.model.MentionEntity;
import com.twitter.clientlib.model.Point;
import com.twitter.clientlib.model.TweetEditControls;
import com.twitter.clientlib.model.TweetGeo;
import com.twitter.clientlib.model.TweetNonPublicMetrics;
import com.twitter.clientlib.model.TweetOrganicMetrics;
import com.twitter.clientlib.model.TweetPromotedMetrics;
import com.twitter.clientlib.model.TweetPublicMetrics;
import com.twitter.clientlib.model.TweetReferencedTweets;
import com.twitter.clientlib.model.UrlEntity;
import com.twitter.clientlib.model.UrlImage;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.kafka.connect.data.Struct;

/** @author <a href="mailto:github@adamcc.ch">Adam Crowther</a> */
class TweetConverterAssertions {

    static void assertNull(final Struct struct, final String field) {
        assertThat(struct.get(field), is(nullValue()));
    }

    static void assertDate(final Struct struct, final String serializedName, final OffsetDateTime expected) {
        final Date actualDate = (Date) struct.get(serializedName);
        final Date expectedDate = new Date(expected.toInstant()
                                                   .toEpochMilli());
        assertThat(actualDate, comparesEqualTo(expectedDate));
    }

    static void assertAttachments(@Nullable final Struct struct) {
        assertThat(struct.getArray(SERIALIZED_NAME_MEDIA_KEYS), contains(MEDIA_KEY));
        assertThat(struct.getArray(SERIALIZED_NAME_POLL_IDS), contains(POLL_ID));
    }

    static void assertReferencedTweets(final List<Struct> structs) {
        assertThat(structs, hasSize(1));
        final Struct referencedTweet = structs.get(0);
        assertThat(referencedTweet.getString(TweetReferencedTweets.SERIALIZED_NAME_ID), is(REFERENCED_TWEET_ID));
        assertThat(referencedTweet.getString(TweetReferencedTweets.SERIALIZED_NAME_TYPE), is(QUOTED.getValue()));
    }

    static void assertContextAnnotations(final List<Struct> structs) {
        assertThat(structs, hasSize(1));
        final Struct contextAnnotation = structs.get(0);
        assertContextAnnotationEntity(contextAnnotation.getStruct(SERIALIZED_NAME_DOMAIN));
        assertContextAnnotationDomain(contextAnnotation.getStruct(SERIALIZED_NAME_ENTITY));
    }

    private static void assertContextAnnotationEntity(final Struct struct) {
        assertThat(struct.getString(ContextAnnotationDomainFields.SERIALIZED_NAME_ID),
                   is(CONTEXT_ANNOTATION_DOMAIN_ID));
        assertThat(struct.getString(ContextAnnotationDomainFields.SERIALIZED_NAME_NAME),
                   is(CONTEXT_ANNOTATION_DOMAIN_NAME));
        assertThat(struct.getString(ContextAnnotationDomainFields.SERIALIZED_NAME_DESCRIPTION),
                   is(CONTEXT_ANNOTATION_DOMAIN_DESCRIPTION));
    }

    private static void assertContextAnnotationDomain(final Struct struct) {
        assertThat(struct.getString(ContextAnnotationEntityFields.SERIALIZED_NAME_ID),
                   is(CONTEXT_ANNOTATION_ENTITY_ID));
        assertThat(struct.getString(ContextAnnotationEntityFields.SERIALIZED_NAME_NAME),
                   is(CONTEXT_ANNOTATION_ENTITY_NAME));
        assertThat(struct.getString(ContextAnnotationEntityFields.SERIALIZED_NAME_DESCRIPTION),
                   is(CONTEXT_ANNOTATION_ENTITY_DESCRIPTION));
    }

    static void assertWithheld(final Struct struct) {
        assertThat(struct.getBoolean(SERIALIZED_NAME_COPYRIGHT), is(true));
        assertThat(struct.getArray(SERIALIZED_NAME_COUNTRY_CODES), contains(WITHHELD_COUNTRY_CODE));
        assertThat(struct.getString(SERIALIZED_NAME_SCOPE), is(TWEET.getValue()));
    }

    @SuppressWarnings("unchecked")
    static void assertGeo(final Struct struct) {
        assertThat(struct.getString(SERIALIZED_NAME_PLACE_ID), is(GEO_PLACE_ID));

        final Struct point = struct.getStruct(TweetGeo.SERIALIZED_NAME_COORDINATES);
        assertThat(point.getString(Point.SERIALIZED_NAME_TYPE), is(POINT.getValue()));
        final List<BigDecimal> pointCoordinates = point.getArray(Point.SERIALIZED_NAME_COORDINATES);
        assertThat(pointCoordinates,
                   contains(comparesEqualTo(POINT_COORDINATES_LAT), comparesEqualTo(POINT_COORDINATES_LONG)));
    }

    static void assertEntities(final Struct struct) {
        assertAnnotations(struct.getArray(SERIALIZED_NAME_ANNOTATIONS));
        assertUrls(struct.getArray(SERIALIZED_NAME_URLS));
        assertHashtags(struct.getArray(SERIALIZED_NAME_HASHTAGS));
        assertMentions(struct.getArray(SERIALIZED_NAME_MENTIONS));
        assertCashtags(struct.getArray(SERIALIZED_NAME_CASHTAGS));
    }

    private static void assertAnnotations(final List<Struct> structs) {
        assertThat(structs, hasSize(1));
        final Struct annotation = structs.get(0);
        assertThat(annotation.getInt32(FullTextEntitiesAnnotations.SERIALIZED_NAME_START), is(ANNOTATION_START));
        assertThat(annotation.getInt32(FullTextEntitiesAnnotations.SERIALIZED_NAME_END), is(ANNOTATION_END));
        assertThat(annotation.getString(FullTextEntitiesAnnotations.SERIALIZED_NAME_TYPE), is(ANNOTATION_TYPE));
        assertThat(annotation.getFloat64(SERIALIZED_NAME_PROBABILITY), comparesEqualTo(ANNOTATION_PROBABILITY));
        assertThat(annotation.getString(SERIALIZED_NAME_NORMALIZED_TEXT), is(ANNOTATION_NORMALIZED_TEXT));
    }

    private static void assertUrls(final List<Struct> structs) {
        assertThat(structs, hasSize(1));
        final Struct url = structs.get(0);
        assertThat(url.getInt32(UrlEntity.SERIALIZED_NAME_START), is(URL_START));
        assertThat(url.getInt32(UrlEntity.SERIALIZED_NAME_END), is(URL_END));
        assertThat(url.getString(UrlEntity.SERIALIZED_NAME_URL), is(URL_URL));
        assertThat(url.getString(SERIALIZED_NAME_EXPANDED_URL), is(URL_URL));
        assertThat(url.getString(SERIALIZED_NAME_DISPLAY_URL), is(URL_URL));
        assertThat(url.getString(SERIALIZED_NAME_UNWOUND_URL), is(URL_URL));
        assertThat(url.getInt32(SERIALIZED_NAME_STATUS), is(URL_STATUS));
        assertThat(url.getString(SERIALIZED_NAME_TITLE), is(URL_TITLE));
        assertThat(url.getString(SERIALIZED_NAME_DESCRIPTION), is(URL_DESCRIPTION));
        assertThat(url.getString(SERIALIZED_NAME_MEDIA_KEY), is(URL_MEDIA_KEY));
        assertImages(url.getArray(UrlEntity.SERIALIZED_NAME_IMAGES));
    }

    private static void assertImages(final List<Struct> structs) {
        assertThat(structs, hasSize(1));
        final Struct image = structs.get(0);
        assertThat(image.getString(UrlImage.SERIALIZED_NAME_URL), is(IMAGE_URL));
        assertThat(image.getInt32(SERIALIZED_NAME_HEIGHT), is(IMAGE_HEIGHT));
        assertThat(image.getInt32(SERIALIZED_NAME_WIDTH), is(IMAGE_WIDTH));
    }

    private static void assertHashtags(final List<Struct> structs) {
        assertThat(structs, hasSize(1));
        final Struct hashtag = structs.get(0);
        assertThat(hashtag.getInt32(HashtagEntity.SERIALIZED_NAME_START), is(HASHTAG_START));
        assertThat(hashtag.getInt32(HashtagEntity.SERIALIZED_NAME_END), is(HASHTAG_END));
        assertThat(hashtag.getString(HashtagEntity.SERIALIZED_NAME_TAG), is(HASHTAG_TYPE));
    }

    private static void assertMentions(final List<Struct> structs) {
        assertThat(structs, hasSize(1));
        final Struct mention = structs.get(0);
        assertThat(mention.getInt32(MentionEntity.SERIALIZED_NAME_START), is(MENTION_START));
        assertThat(mention.getInt32(MentionEntity.SERIALIZED_NAME_END), is(MENTION_END));
        assertThat(mention.getString(SERIALIZED_NAME_USERNAME), is(MENTION_USERNAME));
        assertThat(mention.getString(SERIALIZED_NAME_ID), is(MENTION_ID));
    }

    private static void assertCashtags(final List<Struct> structs) {
        assertThat(structs, hasSize(1));
        final Struct cashtag = structs.get(0);
        assertThat(cashtag.getInt32(CashtagEntity.SERIALIZED_NAME_START), is(CASHTAG_START));
        assertThat(cashtag.getInt32(CashtagEntity.SERIALIZED_NAME_END), is(CASHTAG_END));
        assertThat(cashtag.getString(CashtagEntity.SERIALIZED_NAME_TAG), is(CASHTAG_TAG));
    }

    static void assertPublicMetrics(final Struct struct) {
        assertThat(struct.getInt32(TweetPublicMetrics.SERIALIZED_NAME_RETWEET_COUNT), is(PUBLIC_METRICS_RETWEET_COUNT));
        assertThat(struct.getInt32(TweetPublicMetrics.SERIALIZED_NAME_REPLY_COUNT), is(PUBLIC_METRICS_REPLY_COUNT));
        assertThat(struct.getInt32(TweetPublicMetrics.SERIALIZED_NAME_LIKE_COUNT), is(PUBLIC_METRICS_LIKE_COUNT));
        assertThat(struct.getInt32(TweetPublicMetrics.SERIALIZED_NAME_QUOTE_COUNT), is(PUBLIC_METRICS_QUOTE_COUNT));
    }

    static void assertNonPublicMetrics(final Struct struct) {
        assertThat(struct.getInt32(TweetNonPublicMetrics.SERIALIZED_NAME_IMPRESSION_COUNT),
                   is(NON_PUBLIC_METRICS_IMPRESSION_COUNT));
    }

    static void assertPromotedMetrics(final Struct struct) {
        assertThat(struct.getInt32(TweetPromotedMetrics.SERIALIZED_NAME_IMPRESSION_COUNT),
                   is(PROMOTED_METRICS_IMPRESSION_COUNT));
        assertThat(struct.getInt32(TweetPromotedMetrics.SERIALIZED_NAME_RETWEET_COUNT),
                   is(PROMOTED_METRICS_RETWEET_COUNT));
        assertThat(struct.getInt32(TweetPromotedMetrics.SERIALIZED_NAME_REPLY_COUNT), is(PROMOTED_METRICS_REPLY_COUNT));
        assertThat(struct.getInt32(TweetPromotedMetrics.SERIALIZED_NAME_LIKE_COUNT), is(PROMOTED_METRICS_LIKE_COUNT));
    }

    static void assertOrganicMetrics(final Struct struct) {
        assertThat(struct.getInt32(TweetOrganicMetrics.SERIALIZED_NAME_IMPRESSION_COUNT),
                   is(ORGANIC_METRICS_IMPRESSION_COUNT));
        assertThat(struct.getInt32(TweetOrganicMetrics.SERIALIZED_NAME_RETWEET_COUNT),
                   is(ORGANIC_METRICS_RETWEET_COUNT));
        assertThat(struct.getInt32(TweetOrganicMetrics.SERIALIZED_NAME_REPLY_COUNT), is(ORGANIC_METRICS_REPLY_COUNT));
        assertThat(struct.getInt32(TweetOrganicMetrics.SERIALIZED_NAME_LIKE_COUNT), is(ORGANIC_METRICS_LIKE_COUNT));
    }

    static void assertEditControls(final Struct struct) {
        assertDate(struct, TweetEditControls.SERIALIZED_NAME_EDITABLE_UNTIL, EDIT_CONTROLS_EDITABLE_UNTIL);
        assertThat(struct.getInt32(TweetEditControls.SERIALIZED_NAME_EDITS_REMAINING),
                   is(EDIT_CONTROLS_EDITS_REMAINING));
        assertThat(struct.getBoolean(TweetEditControls.SERIALIZED_NAME_IS_EDIT_ELIGIBLE),
                   is(EDIT_CONTROLS_IS_EDIT_ELIGIBLE));
    }

    static void assertEditHistoryTweetIds(final List<String> structs) {
        assertThat(structs, hasSize(1));
        final String edit = structs.get(0);
        assertThat(edit, is(EDIT_HISTORY_TWEET_ID));
    }

}
