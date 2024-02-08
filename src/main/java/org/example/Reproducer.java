package org.example;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.*;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class Reproducer {
    private static final String PROJECT_ID = "test-project";
    private static final String TOPIC_ID = "test-topic";
    private static final String SUBSCRIPTION_ID = "test-subscription";
    private static final TransportChannelProvider channelProvider;
    private static final CredentialsProvider credentialsProvider;

    static {
        String hostport = System.getenv("PUBSUB_EMULATOR_HOST");
        ManagedChannel channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext().build();
        channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        credentialsProvider = NoCredentialsProvider.create();
    }

    public static void main(String... args) throws Exception {
        createTopicIfNotExists();
        createSubscriptionIfNotExists();
        publishMessages();
        subscribeAsync();
    }

    public static void createTopicIfNotExists() throws IOException {
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create(
                TopicAdminSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build()
        )) {
            TopicName topicName = TopicName.of(PROJECT_ID, TOPIC_ID);

            AtomicBoolean exists = new AtomicBoolean(false);
            topicAdminClient.listTopics(ProjectName.of(PROJECT_ID)).iterateAll().forEach(topic -> {
                if (topic.getName().equals(topicName.toString())) {
                    exists.set(true);
                }
            });

            if (!exists.get()) {
                Topic topic = topicAdminClient.createTopic(topicName);
                System.out.println("Created topic: " + topic.getName());
            }
        }
    }

    public static void createSubscriptionIfNotExists() throws IOException {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(
                SubscriptionAdminSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build()
        )) {
            TopicName topicName = TopicName.of(PROJECT_ID, TOPIC_ID);
            SubscriptionName subscriptionName = SubscriptionName.of(PROJECT_ID, SUBSCRIPTION_ID);

            AtomicBoolean exists = new AtomicBoolean(false);
            subscriptionAdminClient.listSubscriptions(ProjectName.of(PROJECT_ID)).iterateAll().forEach(subscription -> {
                if (subscription.getName().equals(subscriptionName.toString())) {
                    exists.set(true);
                }
            });

            if (!exists.get()) {
                Subscription subscription = subscriptionAdminClient.createSubscription(
                        Subscription.newBuilder().setName(subscriptionName.toString())
                                .setTopic(topicName.toString())
                                .setEnableMessageOrdering(true)
                                .build());
                System.out.println("Created pull subscription: " + subscription.getName());
            }
        }
    }

    public static void publishMessages() throws IOException, InterruptedException {
        TopicName topicName = TopicName.of(PROJECT_ID, TOPIC_ID);

        Publisher publisher =
                Publisher.newBuilder(topicName)
                        .setChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .setEndpoint("us-east1-pubsub.googleapis.com:443")
                        .setEnableMessageOrdering(true)
                        .build();

        try {
            Map<String, String> messages = new LinkedHashMap<>();
            messages.put("message1", "key1");
            messages.put("message2", "key1");
            messages.put("message3", "key1");
            messages.put("message4", "key1");

            for (Map.Entry<String, String> entry : messages.entrySet()) {
                ByteString data = ByteString.copyFromUtf8(entry.getKey());
                PubsubMessage pubsubMessage =
                        PubsubMessage.newBuilder().setData(data).setOrderingKey(entry.getValue()).build();
                ApiFuture<String> future = publisher.publish(pubsubMessage);

                ApiFutures.addCallback(
                        future,
                        new ApiFutureCallback<>() {
                            @Override
                            public void onFailure(Throwable throwable) {
                                if (throwable instanceof ApiException apiException) {
                                    System.out.println(apiException.getStatusCode().getCode());
                                    System.out.println(apiException.isRetryable());
                                }
                                System.out.println("Error publishing message : " + pubsubMessage.getData());
                            }

                            @Override
                            public void onSuccess(String messageId) {
                                System.out.println("Published message: " + pubsubMessage.getData() + ", " + messageId);
                            }
                        },
                        MoreExecutors.directExecutor());
            }
        } finally {
            publisher.shutdown();
            publisher.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    public static void subscribeAsync() {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(PROJECT_ID, SUBSCRIPTION_ID);

        MessageReceiver receiver =
                (PubsubMessage message, AckReplyConsumer consumer) -> {
                    System.out.println("Received message: " + message.getData().toStringUtf8() + ", " + message.getMessageId());
                    consumer.ack();
                };

        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, receiver)
                    .setChannelProvider(channelProvider)
                    .setCredentialsProvider(credentialsProvider)
                    .build();
            subscriber.startAsync().awaitRunning();
            System.out.printf("Listening for messages on %s:\n", subscriptionName);
            subscriber.awaitTerminated(30, TimeUnit.SECONDS);
        } catch (TimeoutException timeoutException) {
            subscriber.stopAsync();
        }
    }
}