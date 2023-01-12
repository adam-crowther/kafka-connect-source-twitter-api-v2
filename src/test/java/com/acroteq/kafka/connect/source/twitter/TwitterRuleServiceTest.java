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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.api.TweetsApi;
import com.twitter.clientlib.api.TweetsApi.APIaddOrDeleteRulesRequest;
import com.twitter.clientlib.api.TweetsApi.APIgetRulesRequest;
import com.twitter.clientlib.model.AddOrDeleteRulesRequest;
import com.twitter.clientlib.model.AddOrDeleteRulesResponse;
import com.twitter.clientlib.model.Problem;
import com.twitter.clientlib.model.Rule;
import com.twitter.clientlib.model.RulesLookupResponse;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TwitterRuleServiceTest {

    private static final int RETRIES = 10;

    private static final String KEYWORD = "keyword";
    private static final List<String> KEYWORDS = List.of(KEYWORD);

    private static final String PROBLEM_TITLE = "problemTitle";
    private static final String PROBLEM_TYPE = "problemType";
    private static final String PROBLEM_DETAIL = "problemDetail";
    private static final int PROBLEM_STATUS = 500;

    @Mock private TweetsApi tweetsApi;
    @Mock private APIaddOrDeleteRulesRequest apiAddOrDeleteRulesRequest;
    @Mock private AddOrDeleteRulesResponse addOrDeleteRulesResponse;
    @Mock private APIgetRulesRequest apiGetRulesRequest;
    @Mock private RulesLookupResponse rulesLookupResponse;
    @Mock private ApiException apiException;
    @Mock private TwitterConfig twitterConfig;

    @Mock private Rule rule;

    private TwitterRuleService twitterRuleService;

    @SneakyThrows
    @BeforeEach
    void setUp() {
        when(twitterConfig.getRetries()).thenReturn(RETRIES);
        twitterRuleService = TwitterRuleService.builder()
                                               .tweetsApi(tweetsApi)
                                               .config(twitterConfig)
                                               .build();
    }

    @SneakyThrows
    @Test
    void testAddKeywordFilterRules_noExistingRules() {
        // given:
        mockAddKeywordFilterRule(List.of(rule));
        // when:
        twitterRuleService.addKeywordFilterRules(KEYWORDS);
        // then:
        verify(tweetsApi).addOrDeleteRules(any(AddOrDeleteRulesRequest.class));
    }

    @SneakyThrows
    @Test
    void testAddKeywordFilterRules_withExistingRules() {
        // given:
        mockAddKeywordFilterRule(List.of(rule));
        // when:
        twitterRuleService.addKeywordFilterRules(KEYWORDS);
        // then:
        verify(tweetsApi).addOrDeleteRules(any(AddOrDeleteRulesRequest.class));
    }

    @SneakyThrows
    @Test
    void testAddKeywordFilterRules_ruleNotInserted() {
        // given:
        mockAddKeywordFilterRule(emptyList());
        // when:
        final TwitterException twitterException =
              assertThrows(TwitterException.class, () -> twitterRuleService.addKeywordFilterRules(KEYWORDS));
        // then:
        assertThat(twitterException.getMessage(), is("Expected the active rules to contain 'keyword', but it didn't"));
    }

    @SneakyThrows
    @Test
    void testAddKeywordFilterRules_exceptionThrown() {
        // given:
        when(tweetsApi.addOrDeleteRules(any(AddOrDeleteRulesRequest.class))).thenReturn(apiAddOrDeleteRulesRequest);
        when(apiAddOrDeleteRulesRequest.execute(RETRIES)).thenReturn(addOrDeleteRulesResponse);
        when(apiAddOrDeleteRulesRequest.execute(RETRIES)).thenThrow(apiException);
        // when:
        final TwitterException twitterException =
              assertThrows(TwitterException.class, () -> twitterRuleService.addKeywordFilterRules(KEYWORDS));
        // then:
        assertThat(twitterException.getMessage(), is("Error while calling tweetsApi.addOrDeleteRules()"));
        assertThat(twitterException.getCause(), is(apiException));
    }

    @SneakyThrows
    @Test
    void testAddKeywordFilterRules_withErrorResponse() {
        // given:
        when(tweetsApi.addOrDeleteRules(any(AddOrDeleteRulesRequest.class))).thenReturn(apiAddOrDeleteRulesRequest);
        when(apiAddOrDeleteRulesRequest.execute(RETRIES)).thenReturn(addOrDeleteRulesResponse);
        when(addOrDeleteRulesResponse.getErrors()).thenReturn(List.of(createProblem()));
        // when:
        final TwitterException twitterException =
              assertThrows(TwitterException.class, () -> twitterRuleService.addKeywordFilterRules(KEYWORDS));
        // then:
        assertThat(twitterException.getMessage(), is("Error(s) while adding filter rule 'keyword':  \nproblemDetail"));
    }

    @SneakyThrows
    @Test
    void testDeleteAllRules_ok() {
        // given:
        mockGetRules(List.of(rule));
        mockAddKeywordFilterRule(emptyList());
        // when:
        twitterRuleService.deleteAllRules();
        // then:
        verify(tweetsApi).addOrDeleteRules(any(AddOrDeleteRulesRequest.class));
    }

    @SneakyThrows
    @Test
    void testDeleteAllRules_resultRulesNotEmpty() {
        // given:
        mockGetRules(List.of(rule));
        mockAddKeywordFilterRule(List.of(rule));
        // when:
        final TwitterException twitterException =
              assertThrows(TwitterException.class, () -> twitterRuleService.deleteAllRules());
        // then:
        assertThat(twitterException.getMessage(),
                   is("There should be no active rules, but we still received: keyword"));
    }

    @SneakyThrows
    @Test
    void testDeleteAllRules_exceptionInGetRules() {
        // given:
        when(tweetsApi.getRules()).thenReturn(apiGetRulesRequest);
        when(apiGetRulesRequest.execute(RETRIES)).thenThrow(apiException);
        // when:
        final TwitterException twitterException =
              assertThrows(TwitterException.class, () -> twitterRuleService.deleteAllRules());
        // then:
        assertThat(twitterException.getMessage(), is("Error while calling tweetsApi.getRules()"));
        assertThat(twitterException.getCause(), is(apiException));

    }

    @SneakyThrows
    @Test
    void testDeleteAllRules_exceptionInAddOrDeleteRules() {
        // given:
        mockGetRules(List.of(rule));
        when(tweetsApi.addOrDeleteRules(any(AddOrDeleteRulesRequest.class))).thenReturn(apiAddOrDeleteRulesRequest);
        when(apiAddOrDeleteRulesRequest.execute(RETRIES)).thenThrow(apiException);
        // when:
        final TwitterException twitterException =
              assertThrows(TwitterException.class, () -> twitterRuleService.deleteAllRules());
        // then:
        assertThat(twitterException.getMessage(), is("Error while calling tweetsApi.addOrDeleteRules()"));
        assertThat(twitterException.getCause(), is(apiException));
    }

    @SneakyThrows
    @Test
    void testAddKeywordFilterRules_withErrorResponseFromAddOrDeleteRules() {
        // given:
        mockGetRules(List.of(rule));
        when(tweetsApi.addOrDeleteRules(any(AddOrDeleteRulesRequest.class))).thenReturn(apiAddOrDeleteRulesRequest);
        when(apiAddOrDeleteRulesRequest.execute(RETRIES)).thenThrow(apiException);
        // when:
        final TwitterException twitterException =
              assertThrows(TwitterException.class, () -> twitterRuleService.deleteAllRules());
        // then:
        assertThat(twitterException.getMessage(), is("Error while calling tweetsApi.addOrDeleteRules()"));
        assertThat(twitterException.getCause(), is(apiException));
    }

    private void mockGetRules(final List<Rule> rules) throws ApiException {
        when(tweetsApi.getRules()).thenReturn(apiGetRulesRequest);
        when(apiGetRulesRequest.execute(RETRIES)).thenReturn(rulesLookupResponse);
        when(rulesLookupResponse.getData()).thenReturn(rules);

        if (!rules.isEmpty()) {
            when(rule.getValue()).thenReturn(KEYWORD);
        }
    }

    private void mockAddKeywordFilterRule(final List<Rule> rules) throws ApiException {
        when(tweetsApi.addOrDeleteRules(any(AddOrDeleteRulesRequest.class))).thenReturn(apiAddOrDeleteRulesRequest);
        when(apiAddOrDeleteRulesRequest.execute(RETRIES)).thenReturn(addOrDeleteRulesResponse);
        when(addOrDeleteRulesResponse.getData()).thenReturn(rules);

        if (!rules.isEmpty()) {
            when(rule.getValue()).thenReturn(KEYWORD);
        }
    }

    private Problem createProblem() {
        return new Problem().title(PROBLEM_TITLE)
                            .type(PROBLEM_TYPE)
                            .detail(PROBLEM_DETAIL)
                            .status(PROBLEM_STATUS);
    }
}