package com.acroteq.kafka.connect.source.converter;

import static com.twitter.clientlib.model.Point.TypeEnum.POINT;
import static com.twitter.clientlib.model.TweetReferencedTweets.TypeEnum.QUOTED;
import static com.twitter.clientlib.model.TweetWithheld.ScopeEnum.TWEET;
import static java.time.ZoneOffset.UTC;

import com.twitter.clientlib.model.CashtagEntity;
import com.twitter.clientlib.model.ContextAnnotation;
import com.twitter.clientlib.model.ContextAnnotationDomainFields;
import com.twitter.clientlib.model.ContextAnnotationEntityFields;
import com.twitter.clientlib.model.FullTextEntities;
import com.twitter.clientlib.model.FullTextEntitiesAnnotations;
import com.twitter.clientlib.model.HashtagEntity;
import com.twitter.clientlib.model.MentionEntity;
import com.twitter.clientlib.model.Point;
import com.twitter.clientlib.model.ReplySettings;
import com.twitter.clientlib.model.Tweet;
import com.twitter.clientlib.model.TweetAttachments;
import com.twitter.clientlib.model.TweetEditControls;
import com.twitter.clientlib.model.TweetGeo;
import com.twitter.clientlib.model.TweetNonPublicMetrics;
import com.twitter.clientlib.model.TweetOrganicMetrics;
import com.twitter.clientlib.model.TweetPromotedMetrics;
import com.twitter.clientlib.model.TweetPublicMetrics;
import com.twitter.clientlib.model.TweetReferencedTweets;
import com.twitter.clientlib.model.TweetWithheld;
import com.twitter.clientlib.model.UrlEntity;
import com.twitter.clientlib.model.UrlImage;
import java.math.BigDecimal;
import java.net.URL;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import lombok.SneakyThrows;

class TweetTestDataGenerator {

    static final String TWEET_ID = "tweet-id";
    static final OffsetDateTime CREATED_AT = OffsetDateTime.of(2023, 1, 10, 14, 29, 22, 0, UTC);
    static final String TWEET_TEXT = "tweet-text";
    static final String AUTHOR_ID = "author-id";
    static final String IN_REPLY_TO_USER_ID = "in-reply-to-user-id";
    static final String LANG = "lang";
    static final String SOURCE = "source";
    static final String CONVERSATION_ID = "conversation-id";
    static final String EDIT_HISTORY_TWEET_ID = "edit-history-tweet-id";
    static final String MEDIA_KEY = "media-key";
    static final String POLL_ID = "poll-id";
    static final String REFERENCED_TWEET_ID = "referenced-tweet-id";
    static final String CONTEXT_ANNOTATION_DOMAIN_ID = "context-annotation-domain-id";
    static final String CONTEXT_ANNOTATION_DOMAIN_NAME = "context-annotation-domain-name";
    static final String CONTEXT_ANNOTATION_DOMAIN_DESCRIPTION = "context-annotation-domain-description";
    static final String CONTEXT_ANNOTATION_ENTITY_ID = "context-annotation-entity-id";
    static final String CONTEXT_ANNOTATION_ENTITY_NAME = "context-annotation-entity-name";
    static final String CONTEXT_ANNOTATION_ENTITY_DESCRIPTION = "context-annotation-entity-description";
    static final String WITHHELD_COUNTRY_CODE = "withheld-country-code";
    static final String GEO_PLACE_ID = "Hermaness";
    static final BigDecimal POINT_COORDINATES_LAT = new BigDecimal("60.8442265");
    static final BigDecimal POINT_COORDINATES_LONG = new BigDecimal("-0.8899802");
    static final int ANNOTATION_START = 10;
    static final int ANNOTATION_END = 11;
    static final String ANNOTATION_TYPE = "annotation-type";
    static final double ANNOTATION_PROBABILITY = 0.12;
    static final String ANNOTATION_NORMALIZED_TEXT = "annotation-normalized-text";
    static final int URL_START = 20;
    static final int URL_END = 21;
    static final String URL_URL = "http://www.adamcc.ch/zenphoto/birds/";
    static final int URL_STATUS = 200;
    static final String URL_TITLE = "url-title";
    static final String URL_DESCRIPTION = "url-description";
    static final String URL_MEDIA_KEY = "url-media-key";
    static final String IMAGE_URL = "https://adamcc.ch/zenphoto/birds/bienenfresser/DSC_6328.jpg";
    static final int IMAGE_HEIGHT = 397;
    static final int IMAGE_WIDTH = 595;
    static final int HASHTAG_START = 30;
    static final int HASHTAG_END = 31;
    static final String HASHTAG_TYPE = "hashtag-type";
    static final int MENTION_START = 40;
    static final int MENTION_END = 41;
    static final String MENTION_USERNAME = "mention-username";
    static final String MENTION_ID = "mention-id";
    static final int CASHTAG_START = 50;
    static final int CASHTAG_END = 51;
    static final String CASHTAG_TAG = "cashtag-tag";
    static final int PUBLIC_METRICS_RETWEET_COUNT = 1;
    static final int PUBLIC_METRICS_REPLY_COUNT = 2;
    static final int PUBLIC_METRICS_LIKE_COUNT = 3;
    static final int PUBLIC_METRICS_QUOTE_COUNT = 4;
    static final int NON_PUBLIC_METRICS_IMPRESSION_COUNT = 5;
    static final int PROMOTED_METRICS_IMPRESSION_COUNT = 6;
    static final int PROMOTED_METRICS_RETWEET_COUNT = 7;
    static final int PROMOTED_METRICS_REPLY_COUNT = 8;
    static final int PROMOTED_METRICS_LIKE_COUNT = 9;
    static final int ORGANIC_METRICS_IMPRESSION_COUNT = 10;
    static final int ORGANIC_METRICS_RETWEET_COUNT = 11;
    static final int ORGANIC_METRICS_REPLY_COUNT = 12;
    static final int ORGANIC_METRICS_LIKE_COUNT = 13;
    static final OffsetDateTime EDIT_CONTROLS_EDITABLE_UNTIL = OffsetDateTime.of(2025, 1, 10, 15, 49, 51, 0, UTC);
    static final int EDIT_CONTROLS_EDITS_REMAINING = 15;
    static final boolean EDIT_CONTROLS_IS_EDIT_ELIGIBLE = true;


    Tweet createTweet() {
        return new Tweet().id(TWEET_ID)
                          .createdAt(CREATED_AT)
                          .text(TWEET_TEXT)
                          .authorId(AUTHOR_ID)
                          .inReplyToUserId(IN_REPLY_TO_USER_ID)
                          .referencedTweets(List.of(createReferencedTweets()))
                          .attachments(createAttachments())
                          .contextAnnotations(List.of(createContextAnnotation()))
                          .withheld(createWithheld())
                          .geo(createTweetGeo())
                          .entities(createEntities())
                          .publicMetrics(createPublicMetrics())
                          .possiblySensitive(true)
                          .lang(LANG)
                          .source(SOURCE)
                          .nonPublicMetrics(createNonPublicMetrics())
                          .promotedMetrics(createPromotedMetrics())
                          .organicMetrics(createOrganicMetrics())
                          .conversationId(CONVERSATION_ID)
                          .editControls(createEditControls())
                          .editHistoryTweetIds(List.of(EDIT_HISTORY_TWEET_ID))
                          .replySettings(ReplySettings.EVERYONE);
    }

    TweetAttachments createAttachments() {
        return new TweetAttachments().mediaKeys(List.of(MEDIA_KEY))
                                     .pollIds(List.of(POLL_ID));
    }

    TweetReferencedTweets createReferencedTweets() {
        return new TweetReferencedTweets().id(REFERENCED_TWEET_ID)
                                          .type(QUOTED);
    }

    ContextAnnotation createContextAnnotation() {
        return new ContextAnnotation().domain(createContextAnnotationsDomainFields())
                                      .entity(createContextAnnotationsEntityFields());
    }

    private ContextAnnotationDomainFields createContextAnnotationsDomainFields() {
        return new ContextAnnotationDomainFields().id(CONTEXT_ANNOTATION_DOMAIN_ID)
                                                  .name(CONTEXT_ANNOTATION_DOMAIN_NAME)
                                                  .description(CONTEXT_ANNOTATION_DOMAIN_DESCRIPTION);
    }

    private ContextAnnotationEntityFields createContextAnnotationsEntityFields() {
        return new ContextAnnotationEntityFields().id(CONTEXT_ANNOTATION_ENTITY_ID)
                                                  .name(CONTEXT_ANNOTATION_ENTITY_NAME)
                                                  .description(CONTEXT_ANNOTATION_ENTITY_DESCRIPTION);
    }

    TweetWithheld createWithheld() {
        return new TweetWithheld().copyright(true)
                                  .countryCodes(Set.of(WITHHELD_COUNTRY_CODE))
                                  .scope(TWEET);
    }

    TweetGeo createTweetGeo() {
        return new TweetGeo().coordinates(createPoint())
                             .placeId(GEO_PLACE_ID);
    }

    private Point createPoint() {
        return new Point().type(POINT)
                          .coordinates(List.of(POINT_COORDINATES_LAT, POINT_COORDINATES_LONG));
    }

    FullTextEntities createEntities() {
        return new FullTextEntities().annotations(List.of(createAnnotation()))
                                     .urls(List.of(createUrl()))
                                     .hashtags(List.of(createHashtag()))
                                     .mentions(List.of(createMention()))
                                     .cashtags(List.of(createCashtag()));
    }

    private FullTextEntitiesAnnotations createAnnotation() {
        return new FullTextEntitiesAnnotations().start(ANNOTATION_START)
                                                .end(ANNOTATION_END)
                                                .type(ANNOTATION_TYPE)
                                                .probability(ANNOTATION_PROBABILITY)
                                                .normalizedText(ANNOTATION_NORMALIZED_TEXT);
    }

    private UrlEntity createUrl() {
        return new UrlEntity().start(URL_START)
                              .end(URL_END)
                              .url(createUrl(URL_URL))
                              .expandedUrl(createUrl(URL_URL))
                              .displayUrl(URL_URL)
                              .unwoundUrl(createUrl(URL_URL))
                              .status(URL_STATUS)
                              .title(URL_TITLE)
                              .description(URL_DESCRIPTION)
                              .mediaKey(URL_MEDIA_KEY)
                              .images(List.of(createUrlImage()));
    }

    @SneakyThrows
    URL createUrl(final String url) {
        return new URL(url);
    }

    private UrlImage createUrlImage() {
        return new UrlImage().url(createUrl(IMAGE_URL))
                             .height(IMAGE_HEIGHT)
                             .width(IMAGE_WIDTH);
    }

    private HashtagEntity createHashtag() {
        return new HashtagEntity().start(HASHTAG_START)
                                  .end(HASHTAG_END)
                                  .tag(HASHTAG_TYPE);
    }

    private MentionEntity createMention() {
        return new MentionEntity().start(MENTION_START)
                                  .end(MENTION_END)
                                  .username(MENTION_USERNAME)
                                  .id(MENTION_ID);
    }

    private CashtagEntity createCashtag() {
        return new CashtagEntity().start(CASHTAG_START)
                                  .end(CASHTAG_END)
                                  .tag(CASHTAG_TAG);
    }

    TweetPublicMetrics createPublicMetrics() {
        return new TweetPublicMetrics().retweetCount(PUBLIC_METRICS_RETWEET_COUNT)
                                       .replyCount(PUBLIC_METRICS_REPLY_COUNT)
                                       .likeCount(PUBLIC_METRICS_LIKE_COUNT)
                                       .quoteCount(PUBLIC_METRICS_QUOTE_COUNT);
    }

    TweetNonPublicMetrics createNonPublicMetrics() {
        return new TweetNonPublicMetrics().impressionCount(NON_PUBLIC_METRICS_IMPRESSION_COUNT);
    }

    TweetPromotedMetrics createPromotedMetrics() {
        return new TweetPromotedMetrics().impressionCount(PROMOTED_METRICS_IMPRESSION_COUNT)
                                         .retweetCount(PROMOTED_METRICS_RETWEET_COUNT)
                                         .replyCount(PROMOTED_METRICS_REPLY_COUNT)
                                         .likeCount(PROMOTED_METRICS_LIKE_COUNT);
    }

    TweetOrganicMetrics createOrganicMetrics() {
        return new TweetOrganicMetrics().impressionCount(ORGANIC_METRICS_IMPRESSION_COUNT)
                                        .retweetCount(ORGANIC_METRICS_RETWEET_COUNT)
                                        .replyCount(ORGANIC_METRICS_REPLY_COUNT)
                                        .likeCount(ORGANIC_METRICS_LIKE_COUNT);
    }

    TweetEditControls createEditControls() {
        return new TweetEditControls().editableUntil(EDIT_CONTROLS_EDITABLE_UNTIL)
                                      .editsRemaining(EDIT_CONTROLS_EDITS_REMAINING)
                                      .isEditEligible(EDIT_CONTROLS_IS_EDIT_ELIGIBLE);
    }
}
