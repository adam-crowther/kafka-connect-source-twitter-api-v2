package com.acroteq.kafka.connect.source.converter;

import static com.twitter.clientlib.model.TweetOrganicMetrics.SERIALIZED_NAME_IMPRESSION_COUNT;
import static com.twitter.clientlib.model.TweetOrganicMetrics.SERIALIZED_NAME_LIKE_COUNT;
import static com.twitter.clientlib.model.TweetOrganicMetrics.SERIALIZED_NAME_REPLY_COUNT;
import static com.twitter.clientlib.model.TweetOrganicMetrics.SERIALIZED_NAME_RETWEET_COUNT;
import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;

import com.twitter.clientlib.model.TweetOrganicMetrics;
import java.util.Optional;
import lombok.NonNull;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.Nullable;

public class OrganicMetricsConverter {

    static final Schema TWEET_ORGANIC_METRICS_SCHEMA = SchemaBuilder.struct()
                                                                    .optional()
                                                                    .field(SERIALIZED_NAME_IMPRESSION_COUNT,
                                                                           INT32_SCHEMA)
                                                                    .field(SERIALIZED_NAME_RETWEET_COUNT, INT32_SCHEMA)
                                                                    .field(SERIALIZED_NAME_REPLY_COUNT, INT32_SCHEMA)
                                                                    .field(SERIALIZED_NAME_LIKE_COUNT, INT32_SCHEMA)
                                                                    .build();

    public Struct convert(@NonNull final TweetOrganicMetrics input) {
        return new Struct(TWEET_ORGANIC_METRICS_SCHEMA).put(SERIALIZED_NAME_IMPRESSION_COUNT,
                                                            input.getImpressionCount())
                                                       .put(SERIALIZED_NAME_RETWEET_COUNT, input.getRetweetCount())
                                                       .put(SERIALIZED_NAME_REPLY_COUNT, input.getReplyCount())
                                                       .put(SERIALIZED_NAME_LIKE_COUNT, input.getLikeCount());
    }


    @Nullable
    Struct convertOptional(final TweetOrganicMetrics organicMetrics) {
        return Optional.ofNullable(organicMetrics)
                       .map(this::convert)
                       .orElse(null);
    }
}
