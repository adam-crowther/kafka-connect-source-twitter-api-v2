package com.acroteq.kafka.connect.source.converter;

import static com.acroteq.kafka.connect.source.converter.TweetConverterAssertions.assertAttachments;
import static com.acroteq.kafka.connect.source.converter.TweetConverterAssertions.assertNull;
import static com.twitter.clientlib.model.TweetAttachments.SERIALIZED_NAME_MEDIA_KEYS;
import static com.twitter.clientlib.model.TweetAttachments.SERIALIZED_NAME_POLL_IDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.twitter.clientlib.model.TweetAttachments;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class AttachmentsConverterTest {

    private final TweetTestDataGenerator testDataGenerator = new TweetTestDataGenerator();

    private final AttachmentsConverter attachmentsConverter = new AttachmentsConverter();

    @Test
    public void testConvertOptional_AllFields() {
        // given:
        final TweetAttachments attachments = testDataGenerator.createAttachments();
        // when:
        final Struct struct = attachmentsConverter.convertOptional(attachments);
        // then:
        assertAttachments(struct);
    }

    @Test
    public void testConvertOptional_MinimalFields() {
        // given:
        final TweetAttachments attachments = new TweetAttachments();
        // when:
        final Struct struct = attachmentsConverter.convertOptional(attachments);
        // then:
        assertNull(struct, SERIALIZED_NAME_MEDIA_KEYS);
        assertNull(struct, SERIALIZED_NAME_POLL_IDS);
    }

    @Test
    public void testConvertOptional_nullValue() {
        // when:
        final Struct struct = attachmentsConverter.convertOptional(null);
        // then:
        assertThat(struct, is(nullValue()));
    }
}
