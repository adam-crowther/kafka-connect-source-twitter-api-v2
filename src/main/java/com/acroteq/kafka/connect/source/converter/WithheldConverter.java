package com.acroteq.kafka.connect.source.converter;

import static com.twitter.clientlib.model.TweetWithheld.SERIALIZED_NAME_COPYRIGHT;
import static com.twitter.clientlib.model.TweetWithheld.SERIALIZED_NAME_COUNTRY_CODES;
import static com.twitter.clientlib.model.TweetWithheld.SERIALIZED_NAME_SCOPE;
import static org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.apache.kafka.connect.data.SchemaBuilder.array;
import static org.apache.kafka.connect.data.SchemaBuilder.struct;

import com.twitter.clientlib.model.TweetWithheld;
import com.twitter.clientlib.model.TweetWithheld.ScopeEnum;
import java.util.ArrayList;
import java.util.Optional;
import lombok.NonNull;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.Nullable;

class WithheldConverter {

    static final Schema WITHHELD_SCHEMA = struct().optional()
                                                  .field(SERIALIZED_NAME_COPYRIGHT, BOOLEAN_SCHEMA)
                                                  .field(SERIALIZED_NAME_COUNTRY_CODES, array(STRING_SCHEMA))
                                                  .field(SERIALIZED_NAME_SCOPE, OPTIONAL_STRING_SCHEMA)
                                                  .build();

    private Struct convert(@NonNull final TweetWithheld input) {
        return new Struct(WITHHELD_SCHEMA).put(SERIALIZED_NAME_COPYRIGHT, input.getCopyright())
                                          .put(SERIALIZED_NAME_COUNTRY_CODES, new ArrayList<>(input.getCountryCodes()))
                                          .put(SERIALIZED_NAME_SCOPE, convertOptionalScope(input.getScope()));
    }

    @Nullable
    Struct convertOptional(@Nullable final TweetWithheld withheld) {
        return Optional.ofNullable(withheld)
                       .map(this::convert)
                       .orElse(null);
    }

    @Nullable
    private String convertOptionalScope(final ScopeEnum scope) {
        return Optional.ofNullable(scope)
                       .map(ScopeEnum::getValue)
                       .orElse(null);
    }
}