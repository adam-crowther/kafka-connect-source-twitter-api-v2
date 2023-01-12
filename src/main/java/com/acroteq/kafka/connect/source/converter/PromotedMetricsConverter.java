package com.acroteq.kafka.connect.source.converter;

import static com.twitter.clientlib.model.TweetPromotedMetrics.SERIALIZED_NAME_IMPRESSION_COUNT;
import static com.twitter.clientlib.model.TweetPromotedMetrics.SERIALIZED_NAME_LIKE_COUNT;
import static com.twitter.clientlib.model.TweetPromotedMetrics.SERIALIZED_NAME_REPLY_COUNT;
import static com.twitter.clientlib.model.TweetPromotedMetrics.SERIALIZED_NAME_RETWEET_COUNT;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA;
import static org.apache.kafka.connect.data.SchemaBuilder.struct;

import com.twitter.clientlib.model.TweetPromotedMetrics;
import java.util.Optional;
import lombok.NonNull;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.Nullable;

public class PromotedMetricsConverter {

    static final Schema TWEET_PROMOTED_METRICS_SCHEMA = struct().optional()
                                                                .field(SERIALIZED_NAME_IMPRESSION_COUNT,
                                                                       OPTIONAL_INT32_SCHEMA)
                                                                .field(SERIALIZED_NAME_LIKE_COUNT,
                                                                       OPTIONAL_INT32_SCHEMA)
                                                                .field(SERIALIZED_NAME_REPLY_COUNT,
                                                                       OPTIONAL_INT32_SCHEMA)
                                                                .field(SERIALIZED_NAME_RETWEET_COUNT,
                                                                       OPTIONAL_INT32_SCHEMA)
                                                                .build();

    public Struct convert(@NonNull final TweetPromotedMetrics input) {
        return new Struct(TWEET_PROMOTED_METRICS_SCHEMA).put(SERIALIZED_NAME_IMPRESSION_COUNT,
                                                             input.getImpressionCount())
                                                        .put(SERIALIZED_NAME_LIKE_COUNT, input.getLikeCount())
                                                        .put(SERIALIZED_NAME_REPLY_COUNT, input.getReplyCount())
                                                        .put(SERIALIZED_NAME_RETWEET_COUNT, input.getRetweetCount());
    }

    @Nullable
    Struct convertOptional(final TweetPromotedMetrics promotedMetrics) {
        return Optional.ofNullable(promotedMetrics)
                       .map(this::convert)
                       .orElse(null);
    }

}
