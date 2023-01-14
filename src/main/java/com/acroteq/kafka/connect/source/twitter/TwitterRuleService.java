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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PRIVATE;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.api.TweetsApi;
import com.twitter.clientlib.model.AddOrDeleteRulesRequest;
import com.twitter.clientlib.model.AddOrDeleteRulesResponse;
import com.twitter.clientlib.model.AddRulesRequest;
import com.twitter.clientlib.model.DeleteRulesRequest;
import com.twitter.clientlib.model.DeleteRulesRequestDelete;
import com.twitter.clientlib.model.Problem;
import com.twitter.clientlib.model.Rule;
import com.twitter.clientlib.model.RuleNoId;
import com.twitter.clientlib.model.RulesLookupResponse;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;

/** @author <a href="mailto:github@adamcc.ch">Adam Crowther</a> */
@AllArgsConstructor(access = PRIVATE)
@Slf4j
class TwitterRuleService {

    @NonNull private final TweetsApi tweetsApi;

    private final int retries;

    static TwitterRuleServiceBuilder builder() {
        return new TwitterRuleServiceBuilder();
    }


    /** Add a new rule to the active rule set that filters the output using the given keywords. */
    void addKeywordFilterRules(@NonNull final List<String> keywords) {
        final String rule = String.join(" OR ", keywords);
        log.info("Adding active rule, filtering for keywords: {}", rule);

        final Optional<AddOrDeleteRulesRequest> request = createAddRulesRequest(singletonList(rule));
        final Optional<AddOrDeleteRulesResponse> response = request.map(this::executeAddOrDeleteRulesRequest);

        response.ifPresent(checkResponseForErrors("Error(s) while adding filter rule '" + rule + "'"));
        response.ifPresent(checkNewRuleInserted(rule));
    }

    private Optional<AddOrDeleteRulesRequest> createAddRulesRequest(@NonNull final List<String> rules) {
        return Optional.of(rules)
                       .filter(ObjectUtils::isNotEmpty)
                       .map(this::createAddRulesRequestActualInstance)
                       .map(this::createAddOrDeleteRulesRequest);

    }

    private AddRulesRequest createAddRulesRequestActualInstance(@NonNull final List<String> rules) {
        return rules.stream()
                    .map(v -> new RuleNoId().value(v))
                    .reduce(new AddRulesRequest(), AddRulesRequest::addAddItem, (l, r) -> l);
    }

    private AddOrDeleteRulesRequest createAddOrDeleteRulesRequest(final AddRulesRequest deleteRulesRequest) {
        final AddOrDeleteRulesRequest request = new AddOrDeleteRulesRequest();
        request.setActualInstance(deleteRulesRequest);
        return request;
    }

    private Consumer<AddOrDeleteRulesResponse> checkResponseForErrors(final String errorMessage) {
        return r -> checkResponseForErrors(errorMessage, r);
    }

    private void checkResponseForErrors(final String errorMessage, @NonNull final AddOrDeleteRulesResponse response) {
        final List<Problem> errors = Optional.of(response)
                                             .map(AddOrDeleteRulesResponse::getErrors)
                                             .orElse(emptyList());
        if (!errors.isEmpty()) {
            logErrorsAndThrowTwitterException(errorMessage, errors);
        }
    }

    private void logErrorsAndThrowTwitterException(final String errorMessage, @NonNull final List<Problem> errors) {
        final String errorSummary = errors.stream()
                                          .map(Problem::getDetail)
                                          .collect(joining(", "));

        errors.stream()
              .map(Problem::toString)
              .forEach(e -> log.warn("Received error response from tweetsApi.addOrDeleteRules(): {}", e));

        throw new TwitterException(errorMessage + ":  \n" + errorSummary);
    }

    private Consumer<AddOrDeleteRulesResponse> checkNewRuleInserted(final String rule) {
        return r -> checkNewRuleInserted(rule, r);
    }

    private void checkNewRuleInserted(final String rule, final AddOrDeleteRulesResponse response) {
        final boolean responseContainsNewRule = Optional.of(response)
                                                        .map(AddOrDeleteRulesResponse::getData)
                                                        .stream()
                                                        .flatMap(List::stream)
                                                        .map(Rule::getValue)
                                                        .anyMatch(rule::equals);
        if (!responseContainsNewRule) {
            throw new TwitterException("Expected the active rules to contain '" + rule + "', but it didn't");
        }
    }

    /** Delete all the rules in the active rule set. */
    void deleteAllRules() {
        log.info("Deleting all active rules.");

        final List<String> values = getAllRules();

        final Optional<AddOrDeleteRulesRequest> request = createDeleteRulesRequest(values);
        final Optional<AddOrDeleteRulesResponse> response = request.map(this::executeAddOrDeleteRulesRequest);

        response.ifPresent(checkResponseForErrors("Error(s) while deleting all rules"));
        response.ifPresent(this::checkResponseHasNoExistingRules);
    }

    private void checkResponseHasNoExistingRules(@NonNull final AddOrDeleteRulesResponse response) {
        final List<Rule> rules = Optional.of(response)
                                         .map(AddOrDeleteRulesResponse::getData)
                                         .orElse(emptyList());
        if (!rules.isEmpty()) {
            final String rulesSummary = rules.stream()
                                             .map(Rule::getValue)
                                             .collect(joining(", "));
            throw new TwitterException("There should be no active rules, but we still received: " + rulesSummary);
        }
    }

    private List<String> getAllRules() {
        try {
            final RulesLookupResponse response = tweetsApi.getRules()
                                                          .execute(retries);
            return Optional.ofNullable(response)
                           .map(RulesLookupResponse::getData)
                           .stream()
                           .flatMap(List::stream)
                           .map(Rule::getValue)
                           .collect(toList());
        } catch (final ApiException e) {
            throw new TwitterException("Error while calling tweetsApi.getRules()", e);
        }
    }

    private Optional<AddOrDeleteRulesRequest> createDeleteRulesRequest(final List<String> values) {
        return Optional.of(values)
                       .filter(ObjectUtils::isNotEmpty)
                       .map(v -> new DeleteRulesRequestDelete().values(values))
                       .map(r -> new DeleteRulesRequest().delete(r))
                       .map(this::createAddOrDeleteRulesRequest);
    }

    private AddOrDeleteRulesRequest createAddOrDeleteRulesRequest(final DeleteRulesRequest deleteRulesRequest) {
        final AddOrDeleteRulesRequest request = new AddOrDeleteRulesRequest();
        request.setActualInstance(deleteRulesRequest);
        return request;
    }

    private AddOrDeleteRulesResponse executeAddOrDeleteRulesRequest(@NonNull final AddOrDeleteRulesRequest request) {
        try {
            return tweetsApi.addOrDeleteRules(request)
                            .execute(retries);
        } catch (final ApiException e) {
            throw new TwitterException("Error while calling tweetsApi.addOrDeleteRules()", e);
        }
    }

    public static class TwitterRuleServiceBuilder {

        private TweetsApi tweetsApi;
        private int retries;

        private TwitterRuleServiceBuilder() {
        }

        TwitterRuleServiceBuilder tweetsApi(final TweetsApi tweetsApi) {
            this.tweetsApi = tweetsApi;
            return this;
        }

        public TwitterRuleServiceBuilder config(final TwitterConfig config) {
            retries = config.getRetries();
            return this;
        }

        public TwitterRuleService build() {
            return new TwitterRuleService(tweetsApi, retries);
        }
    }
}
