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

import static java.util.stream.Collectors.toList;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.apache.kafka.connect.data.SchemaBuilder.struct;

import com.twitter.clientlib.model.ContextAnnotation;
import com.twitter.clientlib.model.ContextAnnotationDomainFields;
import com.twitter.clientlib.model.ContextAnnotationEntityFields;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.Nullable;

/** @author <a href="mailto:github@adamcc.ch">Adam Crowther</a> */
class ContextAnnotationsConverter {

    private static final Schema CONTEXT_ANNOTATION_DOMAIN_SCHEMA =
          struct().field(ContextAnnotationDomainFields.SERIALIZED_NAME_ID, STRING_SCHEMA)
                  .field(ContextAnnotationDomainFields.SERIALIZED_NAME_NAME, OPTIONAL_STRING_SCHEMA)
                  .field(ContextAnnotationDomainFields.SERIALIZED_NAME_DESCRIPTION, OPTIONAL_STRING_SCHEMA)
                  .build();

    private static final Schema CONTEXT_ANNOTATION_ENTITY_SCHEMA =
          struct().field(ContextAnnotationEntityFields.SERIALIZED_NAME_ID, STRING_SCHEMA)
                  .field(ContextAnnotationEntityFields.SERIALIZED_NAME_NAME, OPTIONAL_STRING_SCHEMA)
                  .field(ContextAnnotationEntityFields.SERIALIZED_NAME_DESCRIPTION, OPTIONAL_STRING_SCHEMA)
                  .build();

    static final Schema CONTEXT_ANNOTATION_SCHEMA = struct().optional()
                                                            .field(ContextAnnotation.SERIALIZED_NAME_DOMAIN,
                                                                   CONTEXT_ANNOTATION_DOMAIN_SCHEMA)
                                                            .field(ContextAnnotation.SERIALIZED_NAME_ENTITY,
                                                                   CONTEXT_ANNOTATION_ENTITY_SCHEMA)
                                                            .build();

    private Struct convert(@NonNull final ContextAnnotationDomainFields input) {
        return new Struct(CONTEXT_ANNOTATION_DOMAIN_SCHEMA).put(ContextAnnotationDomainFields.SERIALIZED_NAME_ID,
                                                                input.getId())
                                                           .put(ContextAnnotationDomainFields.SERIALIZED_NAME_NAME,
                                                                input.getName())
                                                           .put(ContextAnnotationDomainFields.SERIALIZED_NAME_DESCRIPTION,
                                                                input.getDescription());
    }

    private Struct convert(@NonNull final ContextAnnotationEntityFields input) {
        return new Struct(CONTEXT_ANNOTATION_ENTITY_SCHEMA).put(ContextAnnotationEntityFields.SERIALIZED_NAME_ID,
                                                                input.getId())
                                                           .put(ContextAnnotationEntityFields.SERIALIZED_NAME_NAME,
                                                                input.getName())
                                                           .put(ContextAnnotationEntityFields.SERIALIZED_NAME_DESCRIPTION,
                                                                input.getDescription());
    }

    private Struct convert(@NonNull final ContextAnnotation input) {
        return new Struct(CONTEXT_ANNOTATION_SCHEMA).put(ContextAnnotation.SERIALIZED_NAME_DOMAIN,
                                                         convert(input.getDomain()))
                                                    .put(ContextAnnotation.SERIALIZED_NAME_ENTITY,
                                                         convert(input.getEntity()));
    }

    @Nullable
    List<Struct> convertOptional(final List<ContextAnnotation> contextAnnotations) {
        return Optional.ofNullable(contextAnnotations)
                       .map(list -> list.stream()
                                        .map(this::convert)
                                        .collect(toList()))
                       .orElse(null);
    }
}
