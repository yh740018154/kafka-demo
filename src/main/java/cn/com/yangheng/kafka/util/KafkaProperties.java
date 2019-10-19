package cn.com.yangheng.kafka.util;

import lombok.Data;

import java.util.Properties;

/**
 * @author yangheng
 * @Classname KafkaProperties
 * @Description TODO
 * @Date 2019/10/19 11:02
 * @group smart video north
 */
@Data
public class KafkaProperties {
    public static final String TOPIC = "yangheng";
    private Properties KafkaProperties;

    public KafkaProperties(String type) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        if ("producer".equals(type)) {
            kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        } else if ("consumer".equals(type)) {
            kafkaProperties.put("group.id","defaultGroup");
            kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        }
        this.KafkaProperties = kafkaProperties;
    }
}
