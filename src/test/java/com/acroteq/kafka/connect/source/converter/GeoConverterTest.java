package com.acroteq.kafka.connect.source.converter;

import static com.acroteq.kafka.connect.source.converter.TweetConverterAssertions.assertGeo;
import static com.acroteq.kafka.connect.source.converter.TweetConverterAssertions.assertNull;
import static com.twitter.clientlib.model.TweetGeo.SERIALIZED_NAME_COORDINATES;
import static com.twitter.clientlib.model.TweetGeo.SERIALIZED_NAME_PLACE_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.twitter.clientlib.model.TweetGeo;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class GeoConverterTest {

    private final TweetTestDataGenerator testDataGenerator = new TweetTestDataGenerator();

    private final GeoConverter geoConverter = new GeoConverter();

    @Test
    public void testConvertOptional_AllFields() {
        // given:
        final TweetGeo geo = testDataGenerator.createTweetGeo();
        // when:
        final Struct struct = geoConverter.convertOptional(geo);
        // then:
        assertGeo(struct);
    }

    @Test
    public void testConvertOptional_minimalFields() {
        // given:
        final TweetGeo geo = new TweetGeo();
        // when:
        final Struct struct = geoConverter.convertOptional(geo);
        // then:
        assertNull(struct, SERIALIZED_NAME_PLACE_ID);
        assertNull(struct, SERIALIZED_NAME_COORDINATES);
    }

    @Test
    public void testConvertOptional_nullValue() {
        // when:
        final Struct struct = geoConverter.convertOptional(null);
        // then:
        assertThat(struct, is(nullValue()));
    }
}