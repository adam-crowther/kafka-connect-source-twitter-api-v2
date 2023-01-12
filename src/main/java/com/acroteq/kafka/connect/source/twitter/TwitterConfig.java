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

import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_BEARER_TOKEN_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_FILTER_KEYWORDS_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_RETRIES_CONF;
import static com.acroteq.kafka.connect.source.TwitterV2SourceConnectorConfig.TWITTER_TWEET_FIELDS_CONF;
import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.common.config.types.Password;

@Builder
@Getter
public class TwitterConfig {

    @NonNull Password bearerToken;

    @Builder.Default
    @NonNull List<String> filterKeywords = new ArrayList<>();

    @Builder.Default
    @NonNull Set<String> fields = new HashSet<>();

    @Builder.Default int retries = 10;

    public static TwitterConfig fromSettingsMap(final Map<String, String> settings) {
        final TwitterConfigBuilder builder = TwitterConfig.builder();
        builder.bearerToken(new Password(settings.get(TWITTER_BEARER_TOKEN_CONF)));
        builder.retries(parseInt(settings.get(TWITTER_RETRIES_CONF)));

        Optional.of(settings)
                .map(s -> s.get(TWITTER_FILTER_KEYWORDS_CONF))
                .map(TwitterConfig::getListOfString)
                .ifPresent(builder::filterKeywords);

        Optional.of(settings)
                .map(s -> s.get(TWITTER_TWEET_FIELDS_CONF))
                .map(TwitterConfig::getSetOfString)
                .ifPresent(builder::fields);

        return builder.build();
    }

    @NonNull
    private static Stream<String> splitToStream(@NonNull final String value) {
        return Optional.of(value)
                       .map(s -> s.split(","))
                       .stream()
                       .flatMap(Stream::of)
                       .map(String::trim);
    }

    private static List<String> getListOfString(@NonNull final String value) {
        return splitToStream(value).collect(toList());
    }

    private static Set<String> getSetOfString(@NonNull final String value) {
        return splitToStream(value).collect(toSet());
    }

}
