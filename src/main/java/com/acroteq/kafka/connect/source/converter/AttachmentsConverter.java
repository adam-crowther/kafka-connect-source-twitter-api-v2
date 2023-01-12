package com.acroteq.kafka.connect.source.converter;

import static com.twitter.clientlib.model.TweetAttachments.SERIALIZED_NAME_MEDIA_KEYS;
import static com.twitter.clientlib.model.TweetAttachments.SERIALIZED_NAME_POLL_IDS;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.apache.kafka.connect.data.SchemaBuilder.array;
import static org.apache.kafka.connect.data.SchemaBuilder.struct;

import com.twitter.clientlib.model.TweetAttachments;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.NonNull;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

class AttachmentsConverter {

    static final Schema TWEET_ATTACHMENTS_SCHEMA = struct().optional()
                                                           .field(SERIALIZED_NAME_MEDIA_KEYS,
                                                                  array(STRING_SCHEMA).optional())
                                                           .field(SERIALIZED_NAME_POLL_IDS,
                                                                  array(STRING_SCHEMA).optional())
                                                           .build();

    private Struct convert(@NonNull final TweetAttachments input) {
        return new Struct(TWEET_ATTACHMENTS_SCHEMA).put(SERIALIZED_NAME_MEDIA_KEYS, input.getMediaKeys())
                                                   .put(SERIALIZED_NAME_POLL_IDS, input.getPollIds());
    }

    @Nullable
    Struct convertOptional(@Nullable final TweetAttachments attachments) {
        return Optional.ofNullable(attachments)
                       .map(this::convert)
                       .orElse(null);
    }
}
