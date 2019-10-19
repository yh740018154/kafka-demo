package cn.com.yangheng.kafka.producer;

import cn.com.yangheng.kafka.util.KafkaProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

/**
 * @author yangheng
 * @Classname Producer
 * @Description TODO
 * @Date 2019/10/19 11:15
 * @group smart video north
 */
@Component
public class SynProducer {
    private static final Logger logger= LoggerFactory.getLogger(SynProducer.class);
    private KafkaProducer<String,String> kafkaProducer=new KafkaProducer<String, String>(new KafkaProperties("producer").getKafkaProperties());

   // @Scheduled(fixedRate=5000)
    public void sendMsg(){
        try {
            double random = Math.random()*1000;
            //同步发送消息
            RecordMetadata recordMetadata = kafkaProducer.send(new ProducerRecord<>(KafkaProperties.TOPIC, "key" + random, "value-" + random)).get();
            logger.info("同步生产的消息key是:{}，value是:{}","key"+random,"value-"+random);
            logger.info("同步发送消息，recordMetadata,主题是：{}，偏移量是：{},partition是：{}",recordMetadata.topic(),recordMetadata.offset(),recordMetadata.partition());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
