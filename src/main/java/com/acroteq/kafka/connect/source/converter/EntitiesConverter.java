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

import static com.twitter.clientlib.model.FullTextEntities.SERIALIZED_NAME_ANNOTATIONS;
import static com.twitter.clientlib.model.FullTextEntities.SERIALIZED_NAME_CASHTAGS;
import static com.twitter.clientlib.model.FullTextEntities.SERIALIZED_NAME_HASHTAGS;
import static com.twitter.clientlib.model.FullTextEntities.SERIALIZED_NAME_MENTIONS;
import static com.twitter.clientlib.model.FullTextEntities.SERIALIZED_NAME_URLS;
import static com.twitter.clientlib.model.FullTextEntitiesAnnotations.SERIALIZED_NAME_NORMALIZED_TEXT;
import static com.twitter.clientlib.model.FullTextEntitiesAnnotations.SERIALIZED_NAME_PROBABILITY;
import static com.twitter.clientlib.model.MentionEntity.SERIALIZED_NAME_ID;
import static com.twitter.clientlib.model.MentionEntity.SERIALIZED_NAME_USERNAME;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_DESCRIPTION;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_DISPLAY_URL;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_EXPANDED_URL;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_IMAGES;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_MEDIA_KEY;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_STATUS;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_TITLE;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_UNWOUND_URL;
import static com.twitter.clientlib.model.UrlImage.SERIALIZED_NAME_HEIGHT;
import static com.twitter.clientlib.model.UrlImage.SERIALIZED_NAME_WIDTH;
import static java.util.stream.Collectors.toList;
import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.apache.kafka.connect.data.SchemaBuilder.array;
import static org.apache.kafka.connect.data.SchemaBuilder.struct;

import com.twitter.clientlib.model.CashtagEntity;
import com.twitter.clientlib.model.FullTextEntities;
import com.twitter.clientlib.model.FullTextEntitiesAnnotations;
import com.twitter.clientlib.model.HashtagEntity;
import com.twitter.clientlib.model.MentionEntity;
import com.twitter.clientlib.model.UrlEntity;
import com.twitter.clientlib.model.UrlImage;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.Nullable;

/** @author <a href="mailto:github@adamcc.ch">Adam Crowther</a> */
class EntitiesConverter {

    private static final Schema ANNOTATIONS_SCHEMA =
          struct().field(FullTextEntitiesAnnotations.SERIALIZED_NAME_START, INT32_SCHEMA)
                  .field(FullTextEntitiesAnnotations.SERIALIZED_NAME_END, INT32_SCHEMA)
                  .field(FullTextEntitiesAnnotations.SERIALIZED_NAME_TYPE, OPTIONAL_STRING_SCHEMA)
                  .field(SERIALIZED_NAME_PROBABILITY, OPTIONAL_FLOAT64_SCHEMA)
                  .field(SERIALIZED_NAME_NORMALIZED_TEXT, OPTIONAL_STRING_SCHEMA)
                  .build();

    private static final Schema URL_IMAGE_SCHEMA = struct().field(UrlImage.SERIALIZED_NAME_URL, STRING_SCHEMA)
                                                           .field(SERIALIZED_NAME_HEIGHT, INT32_SCHEMA)
                                                           .field(SERIALIZED_NAME_WIDTH, INT32_SCHEMA)
                                                           .build();

    private static final Schema URL_ENTITY_SCHEMA = struct().field(UrlEntity.SERIALIZED_NAME_START, INT32_SCHEMA)
                                                            .field(UrlEntity.SERIALIZED_NAME_END, INT32_SCHEMA)
                                                            .field(UrlEntity.SERIALIZED_NAME_URL, STRING_SCHEMA)
                                                            .field(SERIALIZED_NAME_EXPANDED_URL, OPTIONAL_STRING_SCHEMA)
                                                            .field(SERIALIZED_NAME_DISPLAY_URL, OPTIONAL_STRING_SCHEMA)
                                                            .field(SERIALIZED_NAME_UNWOUND_URL, OPTIONAL_STRING_SCHEMA)
                                                            .field(SERIALIZED_NAME_STATUS, OPTIONAL_INT32_SCHEMA)
                                                            .field(SERIALIZED_NAME_TITLE, OPTIONAL_STRING_SCHEMA)
                                                            .field(SERIALIZED_NAME_DESCRIPTION, OPTIONAL_STRING_SCHEMA)
                                                            .field(SERIALIZED_NAME_MEDIA_KEY, OPTIONAL_STRING_SCHEMA)
                                                            .field(SERIALIZED_NAME_IMAGES,
                                                                   array(URL_IMAGE_SCHEMA).optional()
                                                                                          .build())
                                                            .build();

    private static final Schema HASHTAG_ENTITY_SCHEMA =
          struct().field(HashtagEntity.SERIALIZED_NAME_START, INT32_SCHEMA)
                  .field(HashtagEntity.SERIALIZED_NAME_END, INT32_SCHEMA)
                  .field(HashtagEntity.SERIALIZED_NAME_TAG, STRING_SCHEMA)
                  .build();

    private static final Schema MENTION_ENTITY_SCHEMA =
          struct().field(MentionEntity.SERIALIZED_NAME_START, INT32_SCHEMA)
                  .field(MentionEntity.SERIALIZED_NAME_END, INT32_SCHEMA)
                  .field(SERIALIZED_NAME_USERNAME, STRING_SCHEMA)
                  .field(SERIALIZED_NAME_ID, STRING_SCHEMA)
                  .build();
    private static final Schema CASHTAG_ENTITY_SCHEMA =
          struct().field(CashtagEntity.SERIALIZED_NAME_START, INT32_SCHEMA)
                  .field(CashtagEntity.SERIALIZED_NAME_END, INT32_SCHEMA)
                  .field(CashtagEntity.SERIALIZED_NAME_TAG, STRING_SCHEMA)
                  .build();

    static final Schema FULL_TEXT_ENTITIES_SCHEMA = struct().optional()
                                                            .field(SERIALIZED_NAME_ANNOTATIONS,
                                                                   array(ANNOTATIONS_SCHEMA).optional()
                                                                                            .build())
                                                            .field(SERIALIZED_NAME_URLS,
                                                                   array(URL_ENTITY_SCHEMA).optional()
                                                                                           .build())
                                                            .field(SERIALIZED_NAME_HASHTAGS,
                                                                   array(HASHTAG_ENTITY_SCHEMA).optional()
                                                                                               .build())
                                                            .field(SERIALIZED_NAME_MENTIONS,
                                                                   array(MENTION_ENTITY_SCHEMA).optional()
                                                                                               .build())
                                                            .field(SERIALIZED_NAME_CASHTAGS,
                                                                   array(CASHTAG_ENTITY_SCHEMA).optional()
                                                                                               .build())
                                                            .build();

    private Struct convert(@NonNull final UrlImage input) {
        return new Struct(URL_IMAGE_SCHEMA).put(UrlImage.SERIALIZED_NAME_URL, convertOptionalUrl(input.getUrl()))
                                           .put(SERIALIZED_NAME_HEIGHT, input.getHeight())
                                           .put(SERIALIZED_NAME_WIDTH, input.getWidth());
    }

    private Struct convert(@NonNull final FullTextEntitiesAnnotations input) {
        return new Struct(ANNOTATIONS_SCHEMA).put(FullTextEntitiesAnnotations.SERIALIZED_NAME_START, input.getStart())
                                             .put(FullTextEntitiesAnnotations.SERIALIZED_NAME_END, input.getEnd())
                                             .put(FullTextEntitiesAnnotations.SERIALIZED_NAME_TYPE, input.getType())
                                             .put(SERIALIZED_NAME_PROBABILITY, input.getProbability())
                                             .put(SERIALIZED_NAME_NORMALIZED_TEXT, input.getNormalizedText());
    }

    private Struct convert(@NonNull final UrlEntity input) {
        return new Struct(URL_ENTITY_SCHEMA).put(UrlEntity.SERIALIZED_NAME_START, input.getStart())
                                            .put(UrlEntity.SERIALIZED_NAME_END, input.getEnd())
                                            .put(UrlEntity.SERIALIZED_NAME_URL,
                                                 input.getUrl()
                                                      .toString())
                                            .put(SERIALIZED_NAME_EXPANDED_URL,
                                                 convertOptionalUrl(input.getExpandedUrl()))
                                            .put(SERIALIZED_NAME_DISPLAY_URL, input.getDisplayUrl())
                                            .put(SERIALIZED_NAME_UNWOUND_URL, convertOptionalUrl(input.getUnwoundUrl()))
                                            .put(SERIALIZED_NAME_STATUS, input.getStatus())
                                            .put(SERIALIZED_NAME_TITLE, input.getTitle())
                                            .put(SERIALIZED_NAME_DESCRIPTION, input.getDescription())
                                            .put(SERIALIZED_NAME_MEDIA_KEY, input.getMediaKey())
                                            .put(SERIALIZED_NAME_IMAGES, convertOptionalUrlImages(input.getImages()));
    }

    private Struct convert(@NonNull final CashtagEntity input) {
        return new Struct(CASHTAG_ENTITY_SCHEMA).put(CashtagEntity.SERIALIZED_NAME_START, input.getStart())
                                                .put(CashtagEntity.SERIALIZED_NAME_END, input.getEnd())
                                                .put(CashtagEntity.SERIALIZED_NAME_TAG, input.getTag());
    }

    private Struct convert(@NonNull final MentionEntity input) {
        return new Struct(MENTION_ENTITY_SCHEMA).put(MentionEntity.SERIALIZED_NAME_START, input.getStart())
                                                .put(MentionEntity.SERIALIZED_NAME_END, input.getEnd())
                                                .put(SERIALIZED_NAME_USERNAME, input.getUsername())
                                                .put(SERIALIZED_NAME_ID, input.getId());
    }

    private Struct convert(@NonNull final HashtagEntity input) {
        return new Struct(HASHTAG_ENTITY_SCHEMA).put(HashtagEntity.SERIALIZED_NAME_START, input.getStart())
                                                .put(HashtagEntity.SERIALIZED_NAME_END, input.getEnd())
                                                .put(HashtagEntity.SERIALIZED_NAME_TAG, input.getTag());
    }

    private Struct convert(@NonNull final FullTextEntities input) {
        return new Struct(FULL_TEXT_ENTITIES_SCHEMA).put(SERIALIZED_NAME_ANNOTATIONS,
                                                         convertOptionalAnnotations(input.getAnnotations()))
                                                    .put(SERIALIZED_NAME_URLS, convertOptionalUrls(input.getUrls()))
                                                    .put(SERIALIZED_NAME_HASHTAGS,
                                                         convertOptionalHashtags(input.getHashtags()))
                                                    .put(SERIALIZED_NAME_MENTIONS,
                                                         convertOptionalMentions(input.getMentions()))
                                                    .put(SERIALIZED_NAME_CASHTAGS,
                                                         convertOptionalCashtags(input.getCashtags()));
    }

    @Nullable
    Struct convertOptional(@Nullable final FullTextEntities entities) {
        return Optional.ofNullable(entities)
                       .map(this::convert)
                       .orElse(null);
    }

    @Nullable
    private String convertOptionalUrl(@Nullable final URL url) {
        return Optional.ofNullable(url)
                       .map(URL::toString)
                       .orElse(null);
    }

    @Nullable
    private List<Struct> convertOptionalAnnotations(@Nullable final List<FullTextEntitiesAnnotations> annotations) {
        return Optional.ofNullable(annotations)
                       .map(list -> list.stream()
                                        .map(this::convert)
                                        .collect(toList()))
                       .orElse(null);
    }

    @Nullable
    private List<Struct> convertOptionalUrlImages(@Nullable final List<UrlImage> images) {
        return Optional.ofNullable(images)
                       .map(list -> list.stream()
                                        .map(this::convert)
                                        .collect(toList()))
                       .orElse(null);
    }

    @Nullable
    private List<Struct> convertOptionalUrls(@Nullable final List<UrlEntity> urls) {
        return Optional.ofNullable(urls)
                       .map(list -> list.stream()
                                        .map(this::convert)
                                        .collect(toList()))
                       .orElse(null);
    }

    @Nullable
    private List<Struct> convertOptionalHashtags(@Nullable final List<HashtagEntity> hashtags) {
        return Optional.ofNullable(hashtags)
                       .map(list -> list.stream()
                                        .map(this::convert)
                                        .collect(toList()))
                       .orElse(null);
    }

    @Nullable
    private List<Struct> convertOptionalMentions(@Nullable final List<MentionEntity> mentions) {
        return Optional.ofNullable(mentions)
                       .map(list -> list.stream()
                                        .map(this::convert)
                                        .collect(toList()))
                       .orElse(null);
    }

    @Nullable
    private List<Struct> convertOptionalCashtags(@Nullable final List<CashtagEntity> cashtags) {
        return Optional.ofNullable(cashtags)
                       .map(list -> list.stream()
                                        .map(this::convert)
                                        .collect(toList()))
                       .orElse(null);
    }
}
