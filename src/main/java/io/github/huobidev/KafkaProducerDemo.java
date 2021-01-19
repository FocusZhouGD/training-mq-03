package io.github.huobidev;

import io.github.huobidev.qinjinwei.ProducerImpl;

public class KafkaProducerDemo {

    public static void main(String[] args) {
        testProducer();
    }

    private static void testProducer() {
        ProducerImpl producer = new ProducerImpl();
        for (int i = 0; i < 10; i++) {
            producer.send(new Order(0L + i, System.currentTimeMillis(), "USD2CNY", 6.5d));
        }
    }
}
