package com.acroteq.kafka.connect.source.converter;

import static com.twitter.clientlib.model.TweetNonPublicMetrics.SERIALIZED_NAME_IMPRESSION_COUNT;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA;
import static org.apache.kafka.connect.data.SchemaBuilder.struct;

import com.twitter.clientlib.model.TweetNonPublicMetrics;
import java.util.Optional;
import lombok.NonNull;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.Nullable;

class NonPublicMetricsConverter {

    static final Schema TWEET_NON_PUBLIC_METRICS_SCHEMA = struct().optional()
                                                                  .field(SERIALIZED_NAME_IMPRESSION_COUNT,
                                                                         OPTIONAL_INT32_SCHEMA)
                                                                  .build();

    private Struct convert(@NonNull final TweetNonPublicMetrics input) {
        return new Struct(TWEET_NON_PUBLIC_METRICS_SCHEMA).put(SERIALIZED_NAME_IMPRESSION_COUNT,
                                                               input.getImpressionCount());
    }


    @Nullable
    Struct convertOptional(final TweetNonPublicMetrics nonPublicMetrics) {
        return Optional.ofNullable(nonPublicMetrics)
                       .map(this::convert)
                       .orElse(null);
    }
}
