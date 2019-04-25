package com.youzan.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.lang.reflect.Executable;
import java.util.Properties;


public class MyProducer {

    private static KafkaProducer<String, String> producer;

    static {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "39.106.113.166:9092");
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
    }


    /**
     * 只发送，不管发送结果，有可能会出现异常，造成消息丢失
     */
    private static void sendMessageForgetResult() {

        ProducerRecord<String, String> record = new ProducerRecord<>(
                "test1", "name", "dsdsd"
        );
        producer.send(record);
        producer.close();
    }


    /**
     * 以同步的方式发送消息，得到返回的数据
     * @throws Exception
     */
    private static void sendMessageSync() throws Exception {

        ProducerRecord<String, String> record = new ProducerRecord<>(
                "test1", "name", "liuxiaoyang2"
        );
        RecordMetadata result = producer.send(record).get();

        System.out.println(result.topic());
        System.out.println(result.partition());
        System.out.println(result.offset());

        producer.close();
    }

    /**
     * 以异步回调的方式 发送消息
     */
    private static void sendMessageCallback() {

        ProducerRecord<String, String> record = new ProducerRecord<>(
                "test1", "name", "ranrong"
        );
        producer.send(record, new MyProducerCallback());

        record = new ProducerRecord<>(
                "test1", "name-x", "changsheng"
        );
        producer.send(record, new MyProducerCallback());

        record = new ProducerRecord<>(
                "test1", "name-y", "baishuhao"
        );
        producer.send(record, new MyProducerCallback());

        record = new ProducerRecord<>(
                "test1", "name-z", "dapang"
        );
        producer.send(record, new MyProducerCallback());

        producer.close();
    }

    /**
     * 回调类
     */
    private static class MyProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {

            if (e != null) {
                e.printStackTrace();

                return;
            }

            System.out.println(recordMetadata.topic());
            System.out.println(recordMetadata.partition());
            System.out.println(recordMetadata.offset());
            System.out.println("Coming in MyProducerCallback");
        }
    }


    public static void main(String[] args) throws Exception {

//        sendMessageForgetResult();
//        sendMessageSync();
       // sendMessageForgetResult();
        //sendMessageSync();

        sendMessageCallback();

        //sendMessageForgetResult();

       // sendMessageSync();
    }
}
