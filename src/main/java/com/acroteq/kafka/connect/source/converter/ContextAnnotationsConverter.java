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
