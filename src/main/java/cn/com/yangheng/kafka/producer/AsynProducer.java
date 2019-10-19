package cn.com.yangheng.kafka.producer;

import cn.com.yangheng.kafka.util.KafkaProperties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.Future;

/**
 * @author yangheng
 * @Classname AsynProducer
 * @Description TODO
 * @Date 2019/10/19 13:33
 * @group smart video north
 */
@Component
public class AsynProducer {
    private static final Logger logger= LoggerFactory.getLogger(AsynProducer.class);
    private KafkaProducer<String, String> kafkaProducer=new KafkaProducer<String, String>(new KafkaProperties("producer").getKafkaProperties());

    @Scheduled(fixedRate=5000)
    public void sendMsg(){
            double random = Math.random()*1000;
            //异步发送消息
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("asyn" + KafkaProperties.TOPIC, "key-"+ random,"value-" + random);
        Future<RecordMetadata> send = kafkaProducer.send(producerRecord, new ProducerCallback());
        logger.info("异步生产的消息key是:{}，value是:{}","key"+random,"value-"+random);
            //logger.info("异步的recordMetadata,主题是：{}，偏移量是：{},partition是：{}",recordMetadata.topic(),recordMetadata.offset(),recordMetadata.partition());

    }

    /**
     * 发生异常时回调
     */
    public class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if(e!=null){
                logger.warn("kafka 发送消息失败，报错信息：{}",e.getMessage());
            }

        }
    }
}
