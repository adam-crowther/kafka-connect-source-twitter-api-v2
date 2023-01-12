package com.acroteq.kafka.connect.source.converter;

import static com.acroteq.kafka.connect.source.converter.TweetConverterAssertions.assertContextAnnotations;
import static com.acroteq.kafka.connect.source.converter.TweetConverterAssertions.assertNull;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.CONTEXT_ANNOTATION_DOMAIN_ID;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.CONTEXT_ANNOTATION_ENTITY_ID;
import static com.twitter.clientlib.model.ContextAnnotation.SERIALIZED_NAME_DOMAIN;
import static com.twitter.clientlib.model.ContextAnnotation.SERIALIZED_NAME_ENTITY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.twitter.clientlib.model.ContextAnnotation;
import com.twitter.clientlib.model.ContextAnnotationDomainFields;
import com.twitter.clientlib.model.ContextAnnotationEntityFields;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class ContextAnnotationsConverterTest {

    private final TweetTestDataGenerator testDataGenerator = new TweetTestDataGenerator();

    private final ContextAnnotationsConverter contextAnnotationsConverter = new ContextAnnotationsConverter();

    @Test
    public void testConvertOptional_AllFields() {
        // given:
        final ContextAnnotation contextAnnotation = testDataGenerator.createContextAnnotation();
        // when:
        final List<Struct> structs = contextAnnotationsConverter.convertOptional(List.of(contextAnnotation));
        // then:
        assertContextAnnotations(structs);
    }

    @Test
    public void testConvertOptional_MinimalFields() {
        // given:
        final ContextAnnotationDomainFields contextAnnotationDomainFields =
              new ContextAnnotationDomainFields().id(CONTEXT_ANNOTATION_DOMAIN_ID);
        final ContextAnnotationEntityFields contextAnnotationEntityFields =
              new ContextAnnotationEntityFields().id(CONTEXT_ANNOTATION_ENTITY_ID);
        final ContextAnnotation contextAnnotation = new ContextAnnotation().domain(contextAnnotationDomainFields)
                                                                           .entity(contextAnnotationEntityFields);
        // when:
        final List<Struct> structs = contextAnnotationsConverter.convertOptional(List.of(contextAnnotation));
        // then:
        assertThat(structs, hasSize(1));
        final Struct struct = structs.get(0);

        final Struct domainStruct = struct.getStruct(SERIALIZED_NAME_DOMAIN);
        assertThat(domainStruct.getString(ContextAnnotationDomainFields.SERIALIZED_NAME_ID),
                   is(CONTEXT_ANNOTATION_DOMAIN_ID));
        assertNull(domainStruct, ContextAnnotationDomainFields.SERIALIZED_NAME_NAME);
        assertNull(domainStruct, ContextAnnotationDomainFields.SERIALIZED_NAME_DESCRIPTION);

        final Struct entityStruct = struct.getStruct(SERIALIZED_NAME_ENTITY);
        assertThat(entityStruct.getString(ContextAnnotationEntityFields.SERIALIZED_NAME_ID),
                   is(CONTEXT_ANNOTATION_ENTITY_ID));
        assertNull(entityStruct, ContextAnnotationEntityFields.SERIALIZED_NAME_NAME);
        assertNull(entityStruct, ContextAnnotationEntityFields.SERIALIZED_NAME_DESCRIPTION);
    }

    @Test
    public void testConvertOptional_nullValue() {
        // when:
        final List<Struct> struct = contextAnnotationsConverter.convertOptional(null);
        // then:
        assertThat(struct, is(nullValue()));
    }
}