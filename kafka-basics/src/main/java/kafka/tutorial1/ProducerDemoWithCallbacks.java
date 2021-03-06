package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacks {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class);

        String bootstrapServers = "127.0.0.1:9092";
//        System.out.println("Hello world!");
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i =0; i<10; i++){
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", "Hello world"+ Integer.toString(i));

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
            });
        }
        // producer record


        // flush data
        producer.flush();

        // close data
        producer.close();
    }

}
