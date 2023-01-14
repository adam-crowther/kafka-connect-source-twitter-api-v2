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

import static com.acroteq.kafka.connect.source.converter.EntitiesConverter.FULL_TEXT_ENTITIES_SCHEMA;
import static com.acroteq.kafka.connect.source.converter.GeoConverter.GEO_SCHEMA;
import static com.acroteq.kafka.connect.source.converter.ReferencedTweetsConverter.TWEET_REFERENCED_TWEETS_ITEM_SCHEMA;
import static com.acroteq.kafka.connect.source.converter.WithheldConverter.WITHHELD_SCHEMA;
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
import static org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.apache.kafka.connect.data.SchemaBuilder.array;
import static org.apache.kafka.connect.data.SchemaBuilder.struct;

import com.twitter.clientlib.model.ContextAnnotation;
import com.twitter.clientlib.model.FullTextEntities;
import com.twitter.clientlib.model.ReplySettings;
import com.twitter.clientlib.model.Tweet;
import com.twitter.clientlib.model.TweetAttachments;
import com.twitter.clientlib.model.TweetEditControls;
import com.twitter.clientlib.model.TweetGeo;
import com.twitter.clientlib.model.TweetNonPublicMetrics;
import com.twitter.clientlib.model.TweetOrganicMetrics;
import com.twitter.clientlib.model.TweetPromotedMetrics;
import com.twitter.clientlib.model.TweetPublicMetrics;
import com.twitter.clientlib.model.TweetReferencedTweets;
import com.twitter.clientlib.model.TweetWithheld;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.jetbrains.annotations.Nullable;

/** @author <a href="mailto:github@adamcc.ch">Adam Crowther</a> */
public class TweetConverter {

    private final AttachmentsConverter attachmentsConverter = new AttachmentsConverter();
    private final ContextAnnotationsConverter contextAnnotationsConverter = new ContextAnnotationsConverter();
    private final EditControlsConverter editControlsConverter = new EditControlsConverter();
    private final EntitiesConverter entitiesConverter = new EntitiesConverter();
    private final GeoConverter geoConverter = new GeoConverter();
    private final NonPublicMetricsConverter nonPublicMetricsConverter = new NonPublicMetricsConverter();
    private final OrganicMetricsConverter organicMetricsConverter = new OrganicMetricsConverter();
    private final PromotedMetricsConverter promotedMetricsConverter = new PromotedMetricsConverter();
    private final PublicMetricsConverter publicMetricsConverter = new PublicMetricsConverter();
    private final ReferencedTweetsConverter referencedTweetsConverter = new ReferencedTweetsConverter();
    private final WithheldConverter withheldConverter = new WithheldConverter();

    public static final Schema TWEET_SCHEMA = //
          struct().field(SERIALIZED_NAME_ID, STRING_SCHEMA)
                  .field(SERIALIZED_NAME_CREATED_AT,
                         Timestamp.builder()
                                  .optional()
                                  .build())
                  .field(SERIALIZED_NAME_TEXT, STRING_SCHEMA)
                  .field(SERIALIZED_NAME_AUTHOR_ID, OPTIONAL_STRING_SCHEMA)
                  .field(SERIALIZED_NAME_IN_REPLY_TO_USER_ID, OPTIONAL_STRING_SCHEMA)
                  .field(SERIALIZED_NAME_REFERENCED_TWEETS,
                         array(TWEET_REFERENCED_TWEETS_ITEM_SCHEMA).optional()
                                                                   .build())
                  .field(SERIALIZED_NAME_ATTACHMENTS, AttachmentsConverter.TWEET_ATTACHMENTS_SCHEMA)
                  .field(SERIALIZED_NAME_CONTEXT_ANNOTATIONS,
                         array(ContextAnnotationsConverter.CONTEXT_ANNOTATION_SCHEMA).optional()
                                                                                     .build())
                  .field(SERIALIZED_NAME_WITHHELD, WITHHELD_SCHEMA)
                  .field(SERIALIZED_NAME_GEO, GEO_SCHEMA)
                  .field(SERIALIZED_NAME_ENTITIES, FULL_TEXT_ENTITIES_SCHEMA)
                  .field(SERIALIZED_NAME_PUBLIC_METRICS, PublicMetricsConverter.TWEET_PUBLIC_METRICS_SCHEMA)
                  .field(SERIALIZED_NAME_POSSIBLY_SENSITIVE, OPTIONAL_BOOLEAN_SCHEMA)
                  .field(SERIALIZED_NAME_LANG, OPTIONAL_STRING_SCHEMA)
                  .field(SERIALIZED_NAME_SOURCE, OPTIONAL_STRING_SCHEMA)
                  .field(SERIALIZED_NAME_NON_PUBLIC_METRICS, NonPublicMetricsConverter.TWEET_NON_PUBLIC_METRICS_SCHEMA)
                  .field(SERIALIZED_NAME_PROMOTED_METRICS, PromotedMetricsConverter.TWEET_PROMOTED_METRICS_SCHEMA)
                  .field(SERIALIZED_NAME_ORGANIC_METRICS, OrganicMetricsConverter.TWEET_ORGANIC_METRICS_SCHEMA)
                  .field(SERIALIZED_NAME_CONVERSATION_ID, OPTIONAL_STRING_SCHEMA)
                  .field(SERIALIZED_NAME_EDIT_CONTROLS, EditControlsConverter.TWEET_EDIT_CONTROLS_SCHEMA)
                  .field(SERIALIZED_NAME_EDIT_HISTORY_TWEET_IDS, array(STRING_SCHEMA))
                  .field(SERIALIZED_NAME_REPLY_SETTINGS, OPTIONAL_STRING_SCHEMA);

    public Struct convert(@NonNull final Tweet tweet) {
        final Struct struct = new Struct(TWEET_SCHEMA);
        struct.put(SERIALIZED_NAME_ID, tweet.getId());
        struct.put(SERIALIZED_NAME_CREATED_AT, ConverterUtils.convertOptionalDate(tweet.getCreatedAt()));
        struct.put(SERIALIZED_NAME_TEXT, tweet.getText());
        struct.put(SERIALIZED_NAME_AUTHOR_ID, tweet.getAuthorId());
        struct.put(SERIALIZED_NAME_IN_REPLY_TO_USER_ID, tweet.getInReplyToUserId());
        struct.put(SERIALIZED_NAME_REFERENCED_TWEETS, convertOptionalReferencedTweets(tweet.getReferencedTweets()));
        struct.put(SERIALIZED_NAME_ATTACHMENTS, convertOptionalAttachments(tweet.getAttachments()));
        struct.put(SERIALIZED_NAME_CONTEXT_ANNOTATIONS,
                   convertOptionalContextAnnotations(tweet.getContextAnnotations()));
        struct.put(SERIALIZED_NAME_WITHHELD, convertOptionalWithheld(tweet.getWithheld()));
        struct.put(SERIALIZED_NAME_GEO, convertOptionalGeo(tweet.getGeo()));
        struct.put(SERIALIZED_NAME_ENTITIES, convertOptionalEntities(tweet.getEntities()));
        struct.put(SERIALIZED_NAME_PUBLIC_METRICS, convertOptionalPublicMetrics(tweet.getPublicMetrics()));
        struct.put(SERIALIZED_NAME_POSSIBLY_SENSITIVE, tweet.getPossiblySensitive());
        struct.put(SERIALIZED_NAME_LANG, tweet.getLang());
        struct.put(SERIALIZED_NAME_SOURCE, tweet.getSource());
        struct.put(SERIALIZED_NAME_NON_PUBLIC_METRICS, convertOptionalNonPublicMetrics(tweet.getNonPublicMetrics()));
        struct.put(SERIALIZED_NAME_PROMOTED_METRICS, convertOptionalPromotedMetrics(tweet.getPromotedMetrics()));
        struct.put(SERIALIZED_NAME_ORGANIC_METRICS, convertOptionalOrganicMetrics(tweet.getOrganicMetrics()));
        struct.put(SERIALIZED_NAME_CONVERSATION_ID, tweet.getConversationId());
        struct.put(SERIALIZED_NAME_EDIT_CONTROLS, convertOptionalEditControls(tweet.getEditControls()));
        struct.put(SERIALIZED_NAME_EDIT_HISTORY_TWEET_IDS, tweet.getEditHistoryTweetIds());
        struct.put(SERIALIZED_NAME_REPLY_SETTINGS, convertOptionalReplySettings(tweet.getReplySettings()));

        return struct;
    }

    @Nullable
    private static String convertOptionalReplySettings(final ReplySettings replySettings) {
        return Optional.ofNullable(replySettings)
                       .map(ReplySettings::getValue)
                       .orElse(null);
    }

    private Struct convertOptionalAttachments(final TweetAttachments input) {
        return attachmentsConverter.convertOptional(input);
    }

    private List<Struct> convertOptionalContextAnnotations(final List<ContextAnnotation> input) {
        return contextAnnotationsConverter.convertOptional(input);
    }

    private Struct convertOptionalEditControls(final TweetEditControls input) {
        return editControlsConverter.convertOptional(input);
    }

    private Struct convertOptionalEntities(final FullTextEntities input) {
        return entitiesConverter.convertOptional(input);
    }

    private Struct convertOptionalGeo(final TweetGeo input) {
        return geoConverter.convertOptional(input);
    }

    private Struct convertOptionalNonPublicMetrics(final TweetNonPublicMetrics input) {
        return nonPublicMetricsConverter.convertOptional(input);
    }

    private Struct convertOptionalOrganicMetrics(final TweetOrganicMetrics input) {
        return organicMetricsConverter.convertOptional(input);
    }

    private Struct convertOptionalPromotedMetrics(final TweetPromotedMetrics input) {
        return promotedMetricsConverter.convertOptional(input);
    }

    private Struct convertOptionalPublicMetrics(final TweetPublicMetrics input) {
        return publicMetricsConverter.convertOptional(input);
    }

    private List<Struct> convertOptionalReferencedTweets(final List<TweetReferencedTweets> input) {
        return referencedTweetsConverter.convertOptional(input);
    }

    private Struct convertOptionalWithheld(final TweetWithheld input) {
        return withheldConverter.convertOptional(input);
    }
}