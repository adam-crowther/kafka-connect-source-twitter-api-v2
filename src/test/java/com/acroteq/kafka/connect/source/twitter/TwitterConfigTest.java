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
package com.acroteq.kafka.connect.source.twitter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Set;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.Test;

class TwitterConfigTest {

    private static final String PASSWORD = "password";
    private static final Password BEARER_TOKEN = new Password(PASSWORD);
    private static final String KEYWORD = "keyword";
    private static final List<String> FILTER_KEYWORDS = List.of(KEYWORD);
    private static final String FIELD = "field";
    private static final Set<String> FIELDS = Set.of(FIELD);
    private static final int RETRIES = 10;

    @Test
    void testAllFields() {
        // when:
        final TwitterConfig config = TwitterConfig.builder()
                                                  .bearerToken(BEARER_TOKEN)
                                                  .filterKeywords(FILTER_KEYWORDS)
                                                  .fields(FIELDS)
                                                  .retries(RETRIES)
                                                  .build();
        // then:
        assertThat(config.getBearerToken()
                         .value(), is(PASSWORD));
        assertThat(config.getFilterKeywords(), contains(KEYWORD));
        assertThat(config.getFields(), contains(FIELD));
        assertThat(config.getRetries(), is(RETRIES));
    }

    @Test
    void testNullBearerToken() {
        // when:
        assertThrows(NullPointerException.class,
                     () -> TwitterConfig.builder()
                                        .bearerToken(null)
                                        .filterKeywords(FILTER_KEYWORDS)
                                        .fields(FIELDS)
                                        .retries(RETRIES)
                                        .build());
    }

    @Test
    void testMissingBearerToken() {
        // when:
        assertThrows(NullPointerException.class,
                     () -> TwitterConfig.builder()
                                        .filterKeywords(FILTER_KEYWORDS)
                                        .fields(FIELDS)
                                        .retries(RETRIES)
                                        .build());
    }

    @Test
    void testNullKeywords() {
        // when:
        assertThrows(NullPointerException.class,
                     () -> TwitterConfig.builder()
                                        .bearerToken(BEARER_TOKEN)
                                        .filterKeywords(null)
                                        .fields(FIELDS)
                                        .retries(RETRIES)
                                        .build());
    }

    @Test
    void testMissingKeywords() {
        // when:
        final TwitterConfig config = TwitterConfig.builder()
                                                  .bearerToken(BEARER_TOKEN)
                                                  .fields(FIELDS)
                                                  .retries(RETRIES)
                                                  .build();
        // then:
        assertThat(config.getBearerToken()
                         .value(), is(PASSWORD));
        assertThat(config.getFilterKeywords(), is(empty()));
        assertThat(config.getFields(), contains(FIELD));
        assertThat(config.getRetries(), is(RETRIES));
    }

    @Test
    void testNullFields() {
        // when:
        assertThrows(NullPointerException.class,
                     () -> TwitterConfig.builder()
                                        .bearerToken(BEARER_TOKEN)
                                        .filterKeywords(null)
                                        .fields(FIELDS)
                                        .retries(RETRIES)
                                        .build());
    }

    @Test
    void testMissingFields() {
        // when:
        final TwitterConfig config = TwitterConfig.builder()
                                                  .bearerToken(BEARER_TOKEN)
                                                  .filterKeywords(FILTER_KEYWORDS)
                                                  .retries(RETRIES)
                                                  .build();
        // then:
        assertThat(config.getBearerToken()
                         .value(), is(PASSWORD));
        assertThat(config.getFilterKeywords(), contains(KEYWORD));
        assertThat(config.getFields(), is(empty()));
        assertThat(config.getRetries(), is(RETRIES));
    }

    @SuppressWarnings("ConstantValue")
    @Test
    void testNullRetries() {
        // given:
        final Integer retries = null;
        // when:
        assertThrows(NullPointerException.class,
                     () -> TwitterConfig.builder()
                                        .bearerToken(BEARER_TOKEN)
                                        .filterKeywords(null)
                                        .fields(FIELDS)
                                        .retries(retries)
                                        .build());
    }

    @Test
    void testMissingRetries() {
        // when:
        final TwitterConfig config = TwitterConfig.builder()
                                                  .bearerToken(BEARER_TOKEN)
                                                  .filterKeywords(FILTER_KEYWORDS)
                                                  .fields(FIELDS)
                                                  .build();
        // then:
        assertThat(config.getBearerToken()
                         .value(), is(PASSWORD));
        assertThat(config.getFilterKeywords(), contains(KEYWORD));
        assertThat(config.getFields(), contains(FIELD));
        assertThat(config.getRetries(), is(10));
    }

}