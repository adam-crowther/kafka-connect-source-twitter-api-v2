/*
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * For more information, please refer to <http://unlicense.org/>
 */
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

/** @author <a href="mailto:github@adamcc.ch">Adam Crowther</a> */
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