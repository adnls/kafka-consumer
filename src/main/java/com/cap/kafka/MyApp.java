package com.cap.kafka;

import java.time.Duration;

public class MyApp {

    final static String TOPIC = "blabla";
    final static String SERVER = "localhost:9092";
    final static String GROUP = "RandomGroup";
    final static Duration PERIOD = Duration.ofSeconds(1);

    public static void main(String[] args) {

        new MyConsumer(GROUP)
                .bootstrap(SERVER)
                .subscribe(TOPIC)
                .poll(PERIOD);

    }
}
