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

/** @author <a href="mailto:github@adamcc.ch">Adam Crowther</a> */
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
