# Pub/Sub Bug Reproducer

This is a simple project to reproduce a bug in the Pub/Sub emulator.

## Steps to reproduce
1. Clone this repository
2. Ensure the Pub/Sub emulator is running with `--project=test-project`
3. Ensure the `PUBSUB_EMULATOR_HOST` environment variable is set correctly
4. Run the Java application with `mvn clean install exec:java`
5. You will see output such as:
```console
Created topic: projects/test-project/topics/test-topic
Created pull subscription: projects/test-project/subscriptions/test-subscription
Published message: <ByteString@655a961a size=8 contents="message1">, 1
Published message: <ByteString@13a36620 size=8 contents="message2">, 2
Published message: <ByteString@6c61a5be size=8 contents="message3">, 3
Published message: <ByteString@69792f4e size=8 contents="message4">, 4
Listening for messages on projects/test-project/subscriptions/test-subscription:
Received message: message3, 3
Received message: message1, 1
Received message: message4, 4
Received message: message2, 2
```

Notice that the messages are not received in the order they were published.

6. In the `pom.xml` file, change the `com.google.cloud:google-cloud-pubsub` dependency to version `1.125.13`
7. Run the Java application again with `mvn clean install exec:java`
8. You will see output such as:
```console
Published message: <ByteString@5cdcacd0 size=8 contents="message1">, 5
Published message: <ByteString@250db401 size=8 contents="message2">, 6
Published message: <ByteString@17d64797 size=8 contents="message3">, 7
Published message: <ByteString@2ed02a14 size=8 contents="message4">, 8
Listening for messages on projects/test-project/subscriptions/test-subscription:
Received message: message1, 5
Received message: message2, 6
Received message: message3, 7
Received message: message4, 8
```

Notice that the messages are now received in the order they were published.