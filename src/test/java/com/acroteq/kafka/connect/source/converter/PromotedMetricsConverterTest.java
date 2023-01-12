package com.acroteq.kafka.connect.source.converter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.twitter.clientlib.model.TweetPromotedMetrics;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class PromotedMetricsConverterTest {

    private final TweetTestDataGenerator testDataGenerator = new TweetTestDataGenerator();

    private final PromotedMetricsConverter promotedMetricsConverter = new PromotedMetricsConverter();

    @Test
    public void testConvertOptional_AllFields() {
        // given:
        final TweetPromotedMetrics promotedMetrics = testDataGenerator.createPromotedMetrics();
        // when:
        final Struct struct = promotedMetricsConverter.convertOptional(promotedMetrics);
        // then:
        TweetConverterAssertions.assertPromotedMetrics(struct);
    }

    @Test
    public void testConvertOptional_MinimalFields() {
        // given:
        final TweetPromotedMetrics promotedMetrics = new TweetPromotedMetrics();
        // when:
        final Struct struct = promotedMetricsConverter.convertOptional(promotedMetrics);
        // then:
        TweetConverterAssertions.assertNull(struct, TweetPromotedMetrics.SERIALIZED_NAME_IMPRESSION_COUNT);
        TweetConverterAssertions.assertNull(struct, TweetPromotedMetrics.SERIALIZED_NAME_RETWEET_COUNT);
        TweetConverterAssertions.assertNull(struct, TweetPromotedMetrics.SERIALIZED_NAME_REPLY_COUNT);
        TweetConverterAssertions.assertNull(struct, TweetPromotedMetrics.SERIALIZED_NAME_LIKE_COUNT);
    }

    @Test
    public void testConvertOptional_nullValue() {
        // when:
        final Struct struct = promotedMetricsConverter.convertOptional(null);
        // then:
        assertThat(struct, is(nullValue()));
    }
}