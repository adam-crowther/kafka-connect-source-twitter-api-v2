package com.acroteq.kafka.connect.source.converter;

import static com.twitter.clientlib.model.TweetPublicMetrics.SERIALIZED_NAME_LIKE_COUNT;
import static com.twitter.clientlib.model.TweetPublicMetrics.SERIALIZED_NAME_QUOTE_COUNT;
import static com.twitter.clientlib.model.TweetPublicMetrics.SERIALIZED_NAME_REPLY_COUNT;
import static com.twitter.clientlib.model.TweetPublicMetrics.SERIALIZED_NAME_RETWEET_COUNT;
import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA;
import static org.apache.kafka.connect.data.SchemaBuilder.struct;

import com.twitter.clientlib.model.TweetPublicMetrics;
import java.util.Optional;
import lombok.NonNull;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.Nullable;

class PublicMetricsConverter {

    static final Schema TWEET_PUBLIC_METRICS_SCHEMA = struct().optional()
                                                              .field(SERIALIZED_NAME_RETWEET_COUNT, INT32_SCHEMA)
                                                              .field(SERIALIZED_NAME_REPLY_COUNT, INT32_SCHEMA)
                                                              .field(SERIALIZED_NAME_LIKE_COUNT, INT32_SCHEMA)
                                                              .field(SERIALIZED_NAME_QUOTE_COUNT, OPTIONAL_INT32_SCHEMA)
                                                              .build();

    private Struct convert(@NonNull final TweetPublicMetrics input) {
        return new Struct(TWEET_PUBLIC_METRICS_SCHEMA).put(SERIALIZED_NAME_RETWEET_COUNT, input.getRetweetCount())
                                                      .put(SERIALIZED_NAME_REPLY_COUNT, input.getReplyCount())
                                                      .put(SERIALIZED_NAME_LIKE_COUNT, input.getLikeCount())
                                                      .put(SERIALIZED_NAME_QUOTE_COUNT, input.getQuoteCount());
    }

    @Nullable
    Struct convertOptional(final TweetPublicMetrics publicMetrics) {
        return Optional.ofNullable(publicMetrics)
                       .map(this::convert)
                       .orElse(null);
    }
}
