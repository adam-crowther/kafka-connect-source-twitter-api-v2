package com.acroteq.kafka.connect.source.converter;

import static com.acroteq.kafka.connect.source.converter.ConverterUtils.convertDate;
import static com.acroteq.kafka.connect.source.converter.ConverterUtils.convertOptionalDate;
import static java.time.ZoneId.systemDefault;
import static java.util.Calendar.JANUARY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.GregorianCalendar;
import org.junit.jupiter.api.Test;

class ConverterUtilsTest {

    private static final int YEAR = 2023;
    private static final int MONTH = 1;
    private static final int DAY_OF_MONTH = 10;
    private static final int HOUR = 18;
    private static final int MINUTE = 48;
    private static final int SECOND = 44;
    private static final int NANO_OF_SECOND = 0;
    private static final LocalDateTime LOCAL_DATE_TIME =
          LocalDateTime.of(YEAR, MONTH, DAY_OF_MONTH, HOUR, MINUTE, SECOND, NANO_OF_SECOND);
    private static final ZoneOffset TIMEZONE = systemDefault().getRules()
                                                              .getOffset(LOCAL_DATE_TIME);
    private static final OffsetDateTime OFFSET_DATE_TIME = OffsetDateTime.of(LOCAL_DATE_TIME, TIMEZONE);

    @Test
    public void testConvertOptionalDate_withValue() {
        // given:
        // when:
        final Date date = convertOptionalDate(OFFSET_DATE_TIME);
        // then:
        final Date expectedDate = new GregorianCalendar(YEAR, JANUARY, DAY_OF_MONTH, HOUR, MINUTE, SECOND).getTime();
        assertThat(date, comparesEqualTo(expectedDate));
    }

    @Test
    public void testConvertOptionalDate_nullValue() {
        // when:
        final Date date = convertOptionalDate(null);
        // then:
        assertThat(date, is(nullValue()));
    }

    @Test
    public void testConvertDate_withValue() {
        // given:
        // when:
        final Date date = convertDate(OFFSET_DATE_TIME);
        // then:
        final Date expectedDate = new GregorianCalendar(YEAR, JANUARY, DAY_OF_MONTH, HOUR, MINUTE, SECOND).getTime();
        assertThat(date, comparesEqualTo(expectedDate));
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    public void testConvertDate_nullValue() {
        assertThrows(NullPointerException.class, () -> convertDate(null));
    }
}