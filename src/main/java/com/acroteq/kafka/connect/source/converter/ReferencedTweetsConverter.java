package com.acroteq.kafka.connect.source.converter;

import static com.twitter.clientlib.model.TweetReferencedTweets.SERIALIZED_NAME_ID;
import static com.twitter.clientlib.model.TweetReferencedTweets.SERIALIZED_NAME_TYPE;
import static java.util.stream.Collectors.toList;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.apache.kafka.connect.data.SchemaBuilder.struct;

import com.twitter.clientlib.model.TweetReferencedTweets;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.Nullable;

class ReferencedTweetsConverter {

    static final Schema TWEET_REFERENCED_TWEETS_ITEM_SCHEMA = struct().field(SERIALIZED_NAME_TYPE, STRING_SCHEMA)
                                                                      .field(SERIALIZED_NAME_ID, STRING_SCHEMA)
                                                                      .build();

    private Struct convert(@NonNull final TweetReferencedTweets input) {
        return new Struct(TWEET_REFERENCED_TWEETS_ITEM_SCHEMA).put(SERIALIZED_NAME_TYPE,
                                                                   input.getType()
                                                                        .getValue())
                                                              .put(SERIALIZED_NAME_ID, input.getId());
    }

    @Nullable
    List<Struct> convertOptional(@Nullable final List<TweetReferencedTweets> referencedTweets) {
        return Optional.ofNullable(referencedTweets)
                       .map(list -> list.stream()
                                        .map(this::convert)
                                        .collect(toList()))
                       .orElse(null);
    }

}
