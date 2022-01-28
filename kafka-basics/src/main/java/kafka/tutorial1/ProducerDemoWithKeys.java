package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        String bootstrapServers = "127.0.0.1:9092";
//        System.out.println("Hello world!");
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        for(int i =0; i<10; i++){

            String topic = "first_topic";
            String value = "Hello world" + Integer.toString(i);
            String key = "id_"+ Integer.toString(i);

            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            logger.info("key:"+ key); // log the key

            // send data --asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute everytime a record is successfully sent or an exception is thrown
                    if (e == null){
                        // record was successfully sent
                        logger.info("Received new metaData. \n"+
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:"+ recordMetadata.offset() + "\n" +
                                "Timestamp:"+ recordMetadata.timestamp());
                    }else {
                        logger.error("Error while processing", e);

                    }
                }
            }).get(); // block asynchronous don't do this in production
        }
        // producer record


        // flush data
        producer.flush();

        // close data
        producer.close();
    }

}
