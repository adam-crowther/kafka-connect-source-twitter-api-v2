package com.acroteq.kafka.connect.source.converter;

import static com.acroteq.kafka.connect.source.converter.TweetConverterAssertions.assertEntities;
import static com.acroteq.kafka.connect.source.converter.TweetConverterAssertions.assertNull;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.ANNOTATION_END;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.ANNOTATION_START;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.URL_END;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.URL_START;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.URL_URL;
import static com.twitter.clientlib.model.FullTextEntities.SERIALIZED_NAME_ANNOTATIONS;
import static com.twitter.clientlib.model.FullTextEntities.SERIALIZED_NAME_CASHTAGS;
import static com.twitter.clientlib.model.FullTextEntities.SERIALIZED_NAME_HASHTAGS;
import static com.twitter.clientlib.model.FullTextEntities.SERIALIZED_NAME_MENTIONS;
import static com.twitter.clientlib.model.FullTextEntities.SERIALIZED_NAME_URLS;
import static com.twitter.clientlib.model.FullTextEntitiesAnnotations.SERIALIZED_NAME_NORMALIZED_TEXT;
import static com.twitter.clientlib.model.FullTextEntitiesAnnotations.SERIALIZED_NAME_PROBABILITY;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_DESCRIPTION;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_DISPLAY_URL;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_EXPANDED_URL;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_IMAGES;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_MEDIA_KEY;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_STATUS;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_TITLE;
import static com.twitter.clientlib.model.UrlEntity.SERIALIZED_NAME_UNWOUND_URL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.twitter.clientlib.model.FullTextEntities;
import com.twitter.clientlib.model.FullTextEntitiesAnnotations;
import com.twitter.clientlib.model.UrlEntity;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class EntitiesConverterTest {

    private final TweetTestDataGenerator testDataGenerator = new TweetTestDataGenerator();

    private final EntitiesConverter entitiesConverter = new EntitiesConverter();

    @Test
    public void testConvertOptional_AllFields() {
        // given:
        final FullTextEntities entities = testDataGenerator.createEntities();
        // when:
        final Struct struct = entitiesConverter.convertOptional(entities);
        // then:
        assertEntities(struct);
    }

    @Test
    public void testConvertOptional_minimalFields() {
        // given:
        final FullTextEntities entities = new FullTextEntities();
        // when:
        final Struct struct = entitiesConverter.convertOptional(entities);
        // then:
        assertNull(struct, SERIALIZED_NAME_ANNOTATIONS);
        assertNull(struct, SERIALIZED_NAME_URLS);
        assertNull(struct, SERIALIZED_NAME_HASHTAGS);
        assertNull(struct, SERIALIZED_NAME_MENTIONS);
        assertNull(struct, SERIALIZED_NAME_CASHTAGS);
    }

    @Test
    public void testConvertOptional_minimalAnnotationFields() {
        // given:
        final FullTextEntitiesAnnotations annotation = new FullTextEntitiesAnnotations().start(ANNOTATION_START)
                                                                                        .end(ANNOTATION_END);
        final FullTextEntities entities = new FullTextEntities().annotations(List.of(annotation));
        // when:
        final Struct struct = entitiesConverter.convertOptional(entities);
        // then:
        final List<Struct> annotationsArray = struct.getArray(SERIALIZED_NAME_ANNOTATIONS);
        assertThat(annotationsArray, hasSize(1));
        final Struct annotationStruct = annotationsArray.get(0);
        assertThat(annotationStruct.getInt32(FullTextEntitiesAnnotations.SERIALIZED_NAME_START), is(ANNOTATION_START));
        assertThat(annotationStruct.getInt32(FullTextEntitiesAnnotations.SERIALIZED_NAME_END), is(ANNOTATION_END));
        assertNull(annotationStruct, FullTextEntitiesAnnotations.SERIALIZED_NAME_TYPE);
        assertNull(annotationStruct, SERIALIZED_NAME_PROBABILITY);
        assertNull(annotationStruct, SERIALIZED_NAME_NORMALIZED_TEXT);

        assertNull(struct, SERIALIZED_NAME_URLS);
        assertNull(struct, SERIALIZED_NAME_HASHTAGS);
        assertNull(struct, SERIALIZED_NAME_MENTIONS);
        assertNull(struct, SERIALIZED_NAME_CASHTAGS);
    }

    @Test
    public void testConvertOptional_minimalUrlFields() {
        // given:
        final UrlEntity url = new UrlEntity().start(URL_START)
                                             .end(URL_END)
                                             .url(testDataGenerator.createUrl(URL_URL));
        final FullTextEntities entities = new FullTextEntities().urls(List.of(url));
        // when:
        final Struct struct = entitiesConverter.convertOptional(entities);
        // then:
        final List<Struct> urlArray = struct.getArray(SERIALIZED_NAME_URLS);

        assertThat(urlArray, hasSize(1));
        final Struct urlStruct = urlArray.get(0);
        assertThat(urlStruct.getInt32(UrlEntity.SERIALIZED_NAME_START), is(URL_START));
        assertThat(urlStruct.getInt32(UrlEntity.SERIALIZED_NAME_END), is(URL_END));
        assertThat(urlStruct.getString(UrlEntity.SERIALIZED_NAME_URL), is(URL_URL));
        assertNull(urlStruct, SERIALIZED_NAME_EXPANDED_URL);
        assertNull(urlStruct, SERIALIZED_NAME_DISPLAY_URL);
        assertNull(urlStruct, SERIALIZED_NAME_UNWOUND_URL);
        assertNull(urlStruct, SERIALIZED_NAME_STATUS);
        assertNull(urlStruct, SERIALIZED_NAME_TITLE);
        assertNull(urlStruct, SERIALIZED_NAME_DESCRIPTION);
        assertNull(urlStruct, SERIALIZED_NAME_MEDIA_KEY);
        assertNull(urlStruct, SERIALIZED_NAME_IMAGES);

        assertNull(struct, SERIALIZED_NAME_ANNOTATIONS);
        assertNull(struct, SERIALIZED_NAME_HASHTAGS);
        assertNull(struct, SERIALIZED_NAME_MENTIONS);
        assertNull(struct, SERIALIZED_NAME_CASHTAGS);
    }

    @Test
    public void testConvertOptional_nullValue() {
        // when:
        final Struct struct = entitiesConverter.convertOptional(null);
        // then:
        assertThat(struct, is(nullValue()));
    }
}