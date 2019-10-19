package cn.com.yangheng.kafka.consumer;

import cn.com.yangheng.kafka.util.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;

/**
 * @author yangheng
 * @Classname Consumer
 * @Description TODO
 * @Date 2019/10/19 11:09
 * @group smart video north
 */
@Component
public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    private KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(new KafkaProperties("consumer").getKafkaProperties());

    public void consumer() {
        //消费者订阅主题
        kafkaConsumer.subscribe(Collections.singletonList("asyn" + KafkaProperties.TOPIC));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                logger.info("接收到consumerRecord，topic是：{}，分区：{}，key:{},value：{}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
            }
        }
    }
}
