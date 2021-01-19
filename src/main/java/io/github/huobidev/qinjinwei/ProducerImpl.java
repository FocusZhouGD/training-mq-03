package io.github.huobidev.qinjinwei;

import com.alibaba.fastjson.JSON;
import io.github.huobidev.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Properties;

public class ProducerImpl implements Producer {


    private Properties pro;
    private KafkaProducer<String, String> producer;
    private final String topic = "order-test1";


    public ProducerImpl() {
        pro = new Properties();
        pro.put("bootstrap.servers", "localhost:9092");
        pro.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pro.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pro.put("acks", "all");
        pro.put("enable.idempotence",true);
        pro.put("retries", "3");
        pro.put("transactional.id", "transactional_1");
        //消息顺序发送
        pro.put("max.in.flight.requests.per.connection", "1");

        producer = new KafkaProducer<String, String>(pro);
        //开启事务支持
        producer.initTransactions();
    }


    @Override
    public void send(Order order) {

        try {
            producer.beginTransaction();
            ProducerRecord record = new ProducerRecord(topic, order.getId().toString(), JSON.toJSONString(order));
            System.out.println("发送的消息: "+JSON.toJSONString(order));
            //异步发送方式一
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    producer.abortTransaction();
                    throw new KafkaException(exception.getMessage() + ",data:" + record);
                }
            });
            producer.commitTransaction();
        } catch (ProducerFencedException e) {
            producer.abortTransaction();
        }

    }

    @Override
    public void close() {
        producer.close();
    }
}
