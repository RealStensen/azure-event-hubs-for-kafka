//Copyright (c) Microsoft Corporation. All rights reserved.
//Licensed under the MIT License.
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.io.FileReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class TestProducer {
    //Change constant to send messages to the desired topic, for this example we use 'test'
    private final static String TOPIC = "test";
        
    private final static int NUM_THREADS = 1;


    public static void main(String... args) throws Exception {
        //Create Kafka Producer
        final Producer<Long, String> producer = createProducer();

        Thread.sleep(5000);

        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

        //Run NUM_THREADS TestDataReporters
        for (int i = 0; i < NUM_THREADS; i++)
            executorService.execute(new TestDataReporter(producer, TOPIC));
    }

    /*
    bootstrap.servers=mynamespace.servicebus.windows.net:9093
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=XXXXXX;SharedAccessKey=XXXXXX";

     */

    private static Producer<Long, String> createProducer() {
        try{
            Properties properties = new Properties();
            properties.load(new FileReader("src/main/resources/producer.config"));

            properties.put("bootstrap.servers", System.getenv("BOOTSTRAP_SERVERS"));
            properties.put("security.protocol", "SASL_SSL");
            properties.put("sasl.mechanism", "PLAIN");
            properties.put("sasl.jaas.config", System.getenv("SASL_JAAS_CONFIG"));

            properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            return new KafkaProducer<>(properties);
        } catch (Exception e){
            System.out.println("Failed to create producer with exception: " + e);
            System.exit(0);
            return null;        //unreachable
        }
    }
}


