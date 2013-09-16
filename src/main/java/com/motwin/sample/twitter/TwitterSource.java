/**
 * 
 */
package com.motwin.sample.twitter;

import java.util.Map;

import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.motwin.streamdata.Jsons;
import com.motwin.streamdata.continuousresult.AbstractContinuousResult;
import com.motwin.streamdata.spi.ContinuousResult;
import com.motwin.streamdata.spi.SourceMetadata;
import com.motwin.streamdata.spi.StreamdataContext;
import com.motwin.streamdata.spi.StreamdataContextAware;
import com.motwin.streamdata.spi.impl.AbstractPushSource;
import com.motwin.streamdata.spi.impl.SourceMetadataImpl;

/**
 * This Source, of Push type, will start subscription to the streaming API of
 * Twitter with the credentials provided in the constructor, using Twitter4j.
 * 
 * This source also implements StreamdataContextAware in order to be able to
 * retreive shared objects contained in the context of Streamdata.
 * 
 */
public class TwitterSource extends AbstractPushSource implements StreamdataContextAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterSource.class);

    private final String        consumerKey;
    private final String        consumerSecret;
    private final String        accessToken;
    private final String        accessTokenSecret;

    private StreamdataContext   context;

    /**
     * TwitterSource constructor. Initialized with the required credentials for
     * Twitter OAuth authentication.
     * 
     * @param aConsumerKey
     *            the consumer key
     * @param aConsumerSecret
     *            the consumer key secret
     * @param anAccessToken
     *            the access token
     * @param anAccessTokenSecret
     *            the access token secret
     */
    public TwitterSource(final String aConsumerKey, final String aConsumerSecret, final String anAccessToken,
            final String anAccessTokenSecret) {
        consumerKey = Preconditions.checkNotNull(aConsumerKey, "aConsumerKey cannot be null");
        consumerSecret = Preconditions.checkNotNull(aConsumerSecret, "aConsumerSecret cannot be null");
        accessToken = Preconditions.checkNotNull(anAccessToken, "anAccessToken cannot be null");
        accessTokenSecret = Preconditions.checkNotNull(anAccessTokenSecret, "anAccessTokenSecret cannot be null");

    }

    @Override
    public SourceMetadata getMetadata() {
        return new SourceMetadataImpl(ImmutableMap.<String, Boolean> of());
    }

    @Override
    public String getSourceType() {
        return "twitter source";
    }

    @Override
    public ContinuousResult load(JsonNode aParameters) throws Exception {
        // Build a fiber using the fiber factory exposed in streamdata context
        Fiber fiber;
        fiber = context.getFiberFactory().create();
        fiber.start();

        // Create the OAuth authentication
        Configuration configuration;
        Authorization authorization;

        configuration = new ConfigurationBuilder()
	        .setOAuthConsumerKey(consumerKey)
        	.setOAuthConsumerSecret(consumerSecret)
        	.setOAuthAccessToken(accessToken)
        	.setOAuthAccessTokenSecret(accessTokenSecret)
        	.setDebugEnabled(true)
        	.build();

        authorization = AuthorizationFactory.getInstance(configuration);

        // Build the TwitterStream using the authentication
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance(authorization);

        // Build the ContinuousResult that will provide the data to the client.
        TwitterContinuousResult result;
        result = new TwitterContinuousResult(fiber, twitterStream, 500);

        result.setStreamdataContext(context);
        result.afterPropertiesSet();

        return result;
    }

    @Override
    public void setStreamdataContext(StreamdataContext aContext) {
        context = Preconditions.checkNotNull(aContext, "aContext cannot be null");

    }

    @Override
    protected void onInit() throws Exception {
        super.onInit();
        // streamdata context must have been injected by Spring when the object
        // is initialized
        Preconditions.checkNotNull(context, "context cannot be null");

    }

    /**
     * The ContinuousResult for Twitter source
     * 
     */
    public class TwitterContinuousResult extends AbstractContinuousResult {
        private final TwitterStream       twitterStream;
        private final Map<Long, JsonNode> statuses;
        private final long                throttleDelay;
        private long                      lastSendTimestamp;

        /**
         * Create a ContinousResult
         * 
         * @param aFiber
         *            The fiber used by streamdata to handle concurrency
         * @param aTwitterStream
         *            The twitter stream
         * @param aThrottleDelay
         *            the throttle delay (max update rate)
         */
        public TwitterContinuousResult(Fiber aFiber, TwitterStream aTwitterStream, long aThrottleDelay) {
            super(aFiber);

            statuses = Maps.newHashMap();
            twitterStream = Preconditions.checkNotNull(aTwitterStream, "aTwitterStream cannot be null");
            lastSendTimestamp = 0L;
            throttleDelay = aThrottleDelay;

            // Add a listener to the twitter stream
            twitterStream.addListener(new StatusListener() {

                @Override
                public void onStatus(Status aStatus) {
                    // Translate the status in a virtual table row
                    ObjectNode status;
                    status = Jsons.newObject();
                    status.put(M.Tweets.ID, aStatus.getId());
                    status.put(M.Tweets.DATE, aStatus.getCreatedAt().getTime());
                    status.put(M.Tweets.TEXT, aStatus.getText());
                    status.put(M.Tweets.SOURCE, aStatus.getSource());
                    status.put(M.Tweets.USER_SCREEN_NAME, aStatus.getUser().getScreenName());

                    // Add the row to the map of statuses
                    statuses.put(aStatus.getId(), status);

                    // Push the result
                    pushResult();
                }

                @Override
                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                    // Remove the status from the map
                    statuses.remove(statusDeletionNotice.getStatusId());

                    // Push the result
                    pushResult();
                }

                @Override
                public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                    LOGGER.debug("Got track limitation notice: {}", numberOfLimitedStatuses);
                }

                @Override
                public void onScrubGeo(long userId, long upToStatusId) {
                    LOGGER.debug("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
                }

                @Override
                public void onStallWarning(StallWarning warning) {
                    LOGGER.debug("Got stall warning:" + warning);
                }

                @Override
                public void onException(Exception ex) {
                    LOGGER.error("Twitter source threw an exception", ex);
                }
            });

        }

        @Override
        protected ListenableFuture<JsonNode> doFetch() {
            // Return the results
            return Futures.immediateFuture(getStatuses());
        }

        @Override
        protected void doStart() {
            // Start listening on random sample of all public statuses.
            twitterStream.sample();

        }

        @Override
        protected void doStop() {
            // Stop the Twitter subscription
            twitterStream.cleanUp();

        }

        /**
         * Push the result to StreamData. The result will be pushed at a max
         * {@link #throttleDelay} rate
         */
        private void pushResult() {
            long now = System.currentTimeMillis();
            if (now - lastSendTimestamp > throttleDelay) {
                lastSendTimestamp = now;
                send(getStatuses());

            }
        }

        /**
         * Get the statuses
         * 
         * @return the virtual table rows.
         */
        private JsonNode getStatuses() {
            JsonNode[] values;
            values = new JsonNode[statuses.size()];
            values = statuses.values().toArray(values);
            return Jsons.newArray(values);
        }
    };
}
