package com.acroteq.kafka.connect.source.converter;

import static com.acroteq.kafka.connect.source.converter.TweetConverterAssertions.assertNull;
import static com.acroteq.kafka.connect.source.converter.TweetConverterAssertions.assertWithheld;
import static com.acroteq.kafka.connect.source.converter.TweetTestDataGenerator.WITHHELD_COUNTRY_CODE;
import static com.twitter.clientlib.model.TweetWithheld.SERIALIZED_NAME_COPYRIGHT;
import static com.twitter.clientlib.model.TweetWithheld.SERIALIZED_NAME_COUNTRY_CODES;
import static com.twitter.clientlib.model.TweetWithheld.SERIALIZED_NAME_SCOPE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.twitter.clientlib.model.TweetWithheld;
import java.util.Set;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class WithheldConverterTest {

    private final TweetTestDataGenerator testDataGenerator = new TweetTestDataGenerator();

    private final WithheldConverter withheldConverter = new WithheldConverter();

    @Test
    public void testConvertOptional_AllFields() {
        // given:
        final TweetWithheld withheld = testDataGenerator.createWithheld();
        // when:
        final Struct struct = withheldConverter.convertOptional(withheld);
        // then:
        assertWithheld(struct);
    }

    @Test
    public void testConvertOptional_MinimalFields() {
        // given:
        final TweetWithheld withheld = new TweetWithheld().copyright(true)
                                                          .countryCodes(Set.of(WITHHELD_COUNTRY_CODE));
        // when:
        final Struct struct = withheldConverter.convertOptional(withheld);
        // then:
        assertThat(struct.getBoolean(SERIALIZED_NAME_COPYRIGHT), is(true));
        assertThat(struct.getArray(SERIALIZED_NAME_COUNTRY_CODES), contains(WITHHELD_COUNTRY_CODE));
        assertNull(struct, SERIALIZED_NAME_SCOPE);
    }

    @Test
    public void testConvertOptional_nullValue() {
        // when:
        final Struct struct = withheldConverter.convertOptional(null);
        // then:
        assertThat(struct, is(nullValue()));
    }
}