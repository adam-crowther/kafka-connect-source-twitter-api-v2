package com.acroteq.kafka.connect.source.converter;

import static com.acroteq.kafka.connect.source.converter.TweetConverterAssertions.assertNull;
import static com.acroteq.kafka.connect.source.converter.TweetConverterAssertions.assertPublicMetrics;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.PUBLIC_METRICS_LIKE_COUNT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.PUBLIC_METRICS_REPLY_COUNT;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.PUBLIC_METRICS_RETWEET_COUNT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.twitter.clientlib.model.TweetPublicMetrics;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class PublicMetricsConverterTest {

    private final TweetTestDataGenerator testDataGenerator = new TweetTestDataGenerator();

    private final PublicMetricsConverter publicMetricsConverter = new PublicMetricsConverter();

    @Test
    public void testConvertOptional_AllFields() {
        // given:
        final TweetPublicMetrics publicMetrics = testDataGenerator.createPublicMetrics();
        // when:
        final Struct struct = publicMetricsConverter.convertOptional(publicMetrics);
        // then:
        assertPublicMetrics(struct);
    }

    @Test
    public void testConvertOptional_MinimalFields() {
        // given:
        final TweetPublicMetrics publicMetrics = new TweetPublicMetrics().retweetCount(PUBLIC_METRICS_RETWEET_COUNT)
                                                                         .replyCount(PUBLIC_METRICS_REPLY_COUNT)
                                                                         .likeCount(PUBLIC_METRICS_LIKE_COUNT);
        // when:
        final Struct struct = publicMetricsConverter.convertOptional(publicMetrics);
        // then:
        assertThat(struct.getInt32(TweetPublicMetrics.SERIALIZED_NAME_RETWEET_COUNT), is(PUBLIC_METRICS_RETWEET_COUNT));
        assertThat(struct.getInt32(TweetPublicMetrics.SERIALIZED_NAME_REPLY_COUNT), is(PUBLIC_METRICS_REPLY_COUNT));
        assertThat(struct.getInt32(TweetPublicMetrics.SERIALIZED_NAME_LIKE_COUNT), is(PUBLIC_METRICS_LIKE_COUNT));
        assertNull(struct, TweetPublicMetrics.SERIALIZED_NAME_QUOTE_COUNT);
    }

    @Test
    public void testConvertOptional_nullValue() {
        // when:
        final Struct struct = publicMetricsConverter.convertOptional(null);
        // then:
        assertThat(struct, is(nullValue()));
    }
}