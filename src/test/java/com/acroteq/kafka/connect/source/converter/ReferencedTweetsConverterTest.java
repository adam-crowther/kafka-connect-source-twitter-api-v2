package com.acroteq.kafka.connect.source.converter;

import static com.acroteq.kafka.connect.source.converter.TweetConverterAssertions.assertReferencedTweets;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.twitter.clientlib.model.TweetReferencedTweets;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class ReferencedTweetsConverterTest {

    private final TweetTestDataGenerator testDataGenerator = new TweetTestDataGenerator();

    private final ReferencedTweetsConverter referencedTweetsConverter = new ReferencedTweetsConverter();

    @Test
    public void testConvertOptional_AllFields() {
        // given:
        final TweetReferencedTweets referencedTweets = testDataGenerator.createReferencedTweets();
        // when:
        final List<Struct> structs = referencedTweetsConverter.convertOptional(List.of(referencedTweets));
        // then:
        assertReferencedTweets(structs);
    }

    @Test
    public void testConvertOptional_nullValue() {
        // when:
        final List<Struct> struct = referencedTweetsConverter.convertOptional(null);
        // then:
        assertThat(struct, is(nullValue()));
    }
}