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
package com.acroteq.kafka.connect.source;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.PASSWORD;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.CompositeValidator;
import org.apache.kafka.common.config.ConfigDef.NonEmptyString;
import org.apache.kafka.common.config.ConfigDef.NonNullValidator;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.types.Password;

/** @author <a href="mailto:github@adamcc.ch">Adam Crowther</a> */
@Slf4j
@Getter
public class TwitterV2SourceConnectorConfig extends AbstractConfig {

    public static final String TWITTER_BEARER_TOKEN_CONF = "twitter.bearerToken";
    private static final String TWITTER_BEARER_TOKEN_DOC = "Twitter bearer token.  A long, cryptic string.";
    public static final Validator TWITTER_BEARER_TOKEN_VALIDATOR =
          CompositeValidator.of(new NonNullValidator(), new NonEmptyString());

    public static final String TWITTER_FILTER_KEYWORDS_CONF = "twitter.filterKeywords";
    private static final String TWITTER_FILTER_KEYWORDS_DOC = "Comma delimited list of Twitter keywords to filter for. "
          + "The twitter API active filter will be creates with the keywords in the same order.";
    public static final Validator TWITTER_FILTER_KEYWORDS_VALIDATOR =
          CompositeValidator.of(new NonNullValidator(), new NonEmptyString());

    public static final String TWITTER_TWEET_FIELDS_CONF = "twitter.tweetFields";
    private static final String TWITTER_TWEET_FIELDS_DOC =
          "Comma delimited list of fields that will be returned. The order does not matter.";
    public static final Validator TWITTER_TWEET_FIELDS_VALIDATOR =
          CompositeValidator.of(new NonNullValidator(), new NonEmptyString());

    public static final String TWITTER_RETRIES_CONF = "twitter.retries";
    private static final String TWITTER_RETRIES_DOC = "The number of times to retry when the Twitter API call fails.";
    public static final int TWITTER_RETRIES_DEFAULT = 10;
    public static final Range TWITTER_RETRIES_VALIDATOR = between(1, 50);

    public static final String KAFKA_TWEETS_TOPIC_CONF = "topic";
    private static final String KAFKA_TWEETS_TOPIC_DOC = "Kafka topic for output. Default 'twitter-tweets'.";

    static final String KAFKA_TWEETS_TOPIC_DEFAULT = "twitter-tweets";
    static final Validator KAFKA_TWEETS_TOPIC_VALIDATOR =
          CompositeValidator.of(new NonNullValidator(), new NonEmptyString());

    public static final String KAFKA_MAX_BATCH_SIZE_CONF = "kafka.batch.maxSize";
    private static final String KAFKA_MAX_BATCH_SIZE_DOC = "The maximum number of records to return in a single batch.";
    public static final int KAFKA_MAX_BATCH_SIZE_DEFAULT = 100;
    public static final Range KAFKA_MAX_BATCH_SIZE_VALIDATOR = between(1, 1000);

    public static final String KAFKA_MAX_BATCH_INTERVAL_MS_CONF = "kafka.batch.maxIntervalMs";
    private static final String KAFKA_MAX_BATCH_INTERVAL_MS_DOC =
          "The maximum interval in ms between batches, if the maximum batch size was not yet reached. Default 1000 ms.";
    public static final int KAFKA_MAX_BATCH_INTERVAL_MS_DEFAULT = 1000;
    public static final Range KAFKA_MAX_BATCH_INTERVAL_MS_VALIDATOR = between(1, 60000);

    public TwitterV2SourceConnectorConfig(final Map<String, String> parsedConfig) {
        super(createConfigDef(), parsedConfig);
        TWITTER_FILTER_KEYWORDS_VALIDATOR.ensureValid(TWITTER_FILTER_KEYWORDS_CONF,
                                                      getString(TWITTER_FILTER_KEYWORDS_CONF));
        TWITTER_TWEET_FIELDS_VALIDATOR.ensureValid(TWITTER_TWEET_FIELDS_CONF, getString(TWITTER_TWEET_FIELDS_CONF));
    }

    static ConfigDef createConfigDef() {
        return new ConfigDef().define(TWITTER_BEARER_TOKEN_CONF, PASSWORD, HIGH, TWITTER_BEARER_TOKEN_DOC)
                              .define(TWITTER_FILTER_KEYWORDS_CONF, STRING, HIGH, TWITTER_FILTER_KEYWORDS_DOC)
                              .define(TWITTER_TWEET_FIELDS_CONF, STRING, HIGH, TWITTER_TWEET_FIELDS_DOC)
                              .define(TWITTER_RETRIES_CONF,
                                      INT,
                                      TWITTER_RETRIES_DEFAULT,
                                      TWITTER_RETRIES_VALIDATOR,
                                      LOW,
                                      TWITTER_RETRIES_DOC)
                              .define(KAFKA_TWEETS_TOPIC_CONF,
                                      STRING,
                                      KAFKA_TWEETS_TOPIC_DEFAULT,
                                      KAFKA_TWEETS_TOPIC_VALIDATOR,
                                      HIGH,
                                      KAFKA_TWEETS_TOPIC_DOC)
                              .define(KAFKA_MAX_BATCH_SIZE_CONF,
                                      INT,
                                      KAFKA_MAX_BATCH_SIZE_DEFAULT,
                                      KAFKA_MAX_BATCH_SIZE_VALIDATOR,
                                      LOW,
                                      KAFKA_MAX_BATCH_SIZE_DOC)
                              .define(KAFKA_MAX_BATCH_INTERVAL_MS_CONF,
                                      INT,
                                      KAFKA_MAX_BATCH_INTERVAL_MS_DEFAULT,
                                      KAFKA_MAX_BATCH_INTERVAL_MS_VALIDATOR,
                                      LOW,
                                      KAFKA_MAX_BATCH_INTERVAL_MS_DOC);
    }

    public String getTopic() {
        return getString(KAFKA_TWEETS_TOPIC_CONF);
    }

    List<String> getFilterKeywords() {
        return getListOfString(TWITTER_FILTER_KEYWORDS_CONF);
    }

    Set<String> getTweetFields() {
        return getSetOfString(TWITTER_TWEET_FIELDS_CONF);
    }

    public int getMaxBatchSize() {
        return getInt(KAFKA_MAX_BATCH_SIZE_CONF);
    }

    public int getMaxBatchIntervalMs() {
        return getInt(KAFKA_MAX_BATCH_INTERVAL_MS_CONF);
    }

    Password getBearerToken() {
        return getPassword(TWITTER_BEARER_TOKEN_CONF);
    }

    int getRetries() {
        return getInt(TWITTER_RETRIES_CONF);
    }

    @SuppressWarnings("SameParameterValue")
    private List<String> getListOfString(@NonNull final String key) {
        return splitToStream(key).collect(toList());
    }

    @SuppressWarnings("SameParameterValue")
    private Set<String> getSetOfString(@NonNull final String key) {
        return splitToStream(key).collect(toSet());
    }

    @NonNull
    private Stream<String> splitToStream(@NonNull final String key) {
        return Optional.of(key)
                       .map(this::getString)
                       .map(s -> s.split(","))
                       .stream()
                       .flatMap(Stream::of)
                       .map(String::trim);
    }
}
