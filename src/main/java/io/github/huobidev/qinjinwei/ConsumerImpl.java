package io.github.huobidev.qinjinwei;

import com.alibaba.fastjson.JSON;
import io.github.huobidev.Order;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class ConsumerImpl implements Consumer{

   private Properties properties;
   private KafkaConsumer<String,String> consumer;
   private final String topic ="order-test1";

   private Set<String> orderSet =new HashSet<>();
   private Map<TopicPartition, OffsetAndMetadata> currentOffsets =new HashMap<>(16);

    private volatile boolean flag = true;



    public ConsumerImpl(){
        properties=new Properties();
        properties.put("group.id","java1-order-test");
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset","latest");
        properties.put("enable.auto.commit",false);
        properties.put("isolation.level","read_committed");
        consumer=new KafkaConsumer<String, String>(properties);

    }

    @Override
    public void consumerOrder() {

        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true){
                ConsumerRecords<String, String> poll = consumer.poll(1);
                for (ConsumerRecord<String, String> o : poll) {
                    Order order = JSON.parseObject(o.value(), Order.class);
                    System.out.println(" Order = "+ order);
                    //处理重复的消息
                    deduplicationOrder(order);

                    currentOffsets.put(new TopicPartition(o.topic(),o.partition()),new OffsetAndMetadata(o.offset()+1,"no metadata"));
                    //异步提交偏移量
                    consumer.commitAsync(currentOffsets, new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            if(exception!=null){
                                exception.printStackTrace();
                            }
                        }
                    });



                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                consumer.commitAsync();
            } catch (Exception e) {
                consumer.close();
            }
        }
    }



    @Override
    public void close() {
        if(this.flag){
            flag=false;
        }
   consumer.close();
    }

    private void deduplicationOrder(Order order) {
        orderSet.add(order.getId().toString());
    }
}
