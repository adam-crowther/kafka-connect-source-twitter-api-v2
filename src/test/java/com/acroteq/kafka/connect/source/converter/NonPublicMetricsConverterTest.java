package com.acroteq.kafka.connect.source.converter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.twitter.clientlib.model.TweetNonPublicMetrics;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class NonPublicMetricsConverterTest {

    private final TweetTestDataGenerator testDataGenerator = new TweetTestDataGenerator();

    private final NonPublicMetricsConverter nonPublicMetricsConverter = new NonPublicMetricsConverter();

    @Test
    public void testConvertOptional_AllFields() {
        // given:
        final TweetNonPublicMetrics nonPublicMetrics = testDataGenerator.createNonPublicMetrics();
        // when:
        final Struct struct = nonPublicMetricsConverter.convertOptional(nonPublicMetrics);
        // then:
        TweetConverterAssertions.assertNonPublicMetrics(struct);
    }

    @Test
    public void testConvertOptional_MinimalFields() {
        // given:
        final TweetNonPublicMetrics nonPublicMetrics = new TweetNonPublicMetrics();
        // when:
        final Struct struct = nonPublicMetricsConverter.convertOptional(nonPublicMetrics);
        // then:
        TweetConverterAssertions.assertNull(struct, TweetNonPublicMetrics.SERIALIZED_NAME_IMPRESSION_COUNT);
    }

    @Test
    public void testConvertOptional_nullValue() {
        // when:
        final Struct struct = nonPublicMetricsConverter.convertOptional(null);
        // then:
        assertThat(struct, is(nullValue()));
    }

}