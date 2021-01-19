package io.github.huobidev;

import io.github.huobidev.qinjinwei.ConsumerImpl;

public class KafkaConsumerDemo {
    public static void main(String[] args) {
        testConsumer();
    }

    private static void testConsumer() {
        ConsumerImpl consumer = new ConsumerImpl();
        consumer.consumerOrder();
    }

}
