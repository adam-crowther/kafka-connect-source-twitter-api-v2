package com.acroteq.kafka.connect.source.converter;

import static com.acroteq.kafka.connect.source.converter.TweetConverterAssertions.assertEditControls;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.twitter.clientlib.model.TweetEditControls;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class EditControlsConverterTest {

    private final TweetTestDataGenerator testDataGenerator = new TweetTestDataGenerator();

    private final EditControlsConverter editControlsConverter = new EditControlsConverter();

    @Test
    public void testConvertOptional_AllFields() {
        // given:
        final TweetEditControls editControls = testDataGenerator.createEditControls();
        // when:
        final Struct struct = editControlsConverter.convertOptional(editControls);
        // then:
        assertEditControls(struct);
    }

    @Test
    public void testConvertOptional_nullValue() {
        // when:
        final Struct struct = editControlsConverter.convertOptional(null);
        // then:
        assertThat(struct, is(nullValue()));
    }
}