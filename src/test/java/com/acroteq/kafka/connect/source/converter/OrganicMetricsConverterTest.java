package com.acroteq.kafka.connect.source.converter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.twitter.clientlib.model.TweetOrganicMetrics;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class OrganicMetricsConverterTest {

    private final TweetTestDataGenerator testDataGenerator = new TweetTestDataGenerator();

    private final OrganicMetricsConverter organicMetricsConverter = new OrganicMetricsConverter();

    @Test
    public void testConvertOptional_AllFields() {
        // given:
        final TweetOrganicMetrics organicMetrics = testDataGenerator.createOrganicMetrics();
        // when:
        final Struct struct = organicMetricsConverter.convertOptional(organicMetrics);
        // then:
        TweetConverterAssertions.assertOrganicMetrics(struct);
    }

    @Test
    public void testConvertOptional_nullValue() {
        // when:
        final Struct struct = organicMetricsConverter.convertOptional(null);
        // then:
        assertThat(struct, is(nullValue()));
    }
}