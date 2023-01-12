package com.acroteq.kafka.connect.source.converter;

import static com.acroteq.kafka.connect.source.converter.ConverterUtils.convertDate;
import static com.twitter.clientlib.model.TweetEditControls.SERIALIZED_NAME_EDITABLE_UNTIL;
import static com.twitter.clientlib.model.TweetEditControls.SERIALIZED_NAME_EDITS_REMAINING;
import static com.twitter.clientlib.model.TweetEditControls.SERIALIZED_NAME_IS_EDIT_ELIGIBLE;
import static org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
import static org.apache.kafka.connect.data.SchemaBuilder.struct;
import static org.apache.kafka.connect.data.Timestamp.SCHEMA;

import com.twitter.clientlib.model.TweetEditControls;
import java.util.Optional;
import lombok.NonNull;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.Nullable;

class EditControlsConverter {

    static final Schema TWEET_EDIT_CONTROLS_SCHEMA = struct().optional()
                                                             .field(SERIALIZED_NAME_EDITABLE_UNTIL, SCHEMA)
                                                             .field(SERIALIZED_NAME_EDITS_REMAINING, INT32_SCHEMA)
                                                             .field(SERIALIZED_NAME_IS_EDIT_ELIGIBLE, BOOLEAN_SCHEMA)
                                                             .build();

    private Struct convert(@NonNull final TweetEditControls input) {
        return new Struct(TWEET_EDIT_CONTROLS_SCHEMA).put(SERIALIZED_NAME_EDITABLE_UNTIL,
                                                          convertDate(input.getEditableUntil()))
                                                     .put(SERIALIZED_NAME_EDITS_REMAINING, input.getEditsRemaining())
                                                     .put(SERIALIZED_NAME_IS_EDIT_ELIGIBLE, input.getIsEditEligible());
    }


    @Nullable
    Struct convertOptional(final TweetEditControls editControls) {
        return Optional.ofNullable(editControls)
                       .map(this::convert)
                       .orElse(null);
    }
}
