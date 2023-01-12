package com.acroteq.kafka.connect.source.twitter;

import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import org.apache.kafka.common.config.types.Password;

public final class TwitterApiFactory {

    private TwitterApiFactory() {
    }

    public static TwitterApiBuilder newTwitterApi() {
        return new TwitterApiBuilder();
    }

    public static class TwitterApiBuilder {

        private TwitterCredentialsBearer credentials;

        public TwitterApiBuilder bearerToken(final Password bearerToken) {
            credentials = new TwitterCredentialsBearer(bearerToken.value());
            return this;
        }

        public TwitterApi build() {
            return new TwitterApi(credentials);
        }
    }
}
