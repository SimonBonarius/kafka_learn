
package kafka;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Properties;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class App {

    //This is to create the constructor
    public App() {

    }

    public static void main(String[] args) throws IOException, UnirestException, InterruptedException {
        new App().run();

    }

    public void run() throws UnirestException, InterruptedException {

        //create data client
        HttpResponse<JsonNode> response = Unirest.get("https://covid-19-data.p.rapidapi.com/totals")
                .header("x-rapidapi-key", "xxxx")
                .header("x-rapidapi-host", "covid-19-data.p.rapidapi.com")
                .asJson();

        System.out.println(response.getStatus());

        //System.out.println(response.getHeaders().get("Content-Type"));
       // System.out.println(response.getBody().toString());

        //Prettifying
       // Gson gson = new GsonBuilder().setPrettyPrinting().create();
       // JsonParser jp = new JsonParser();
       // JsonElement je = jp.parse(response.getBody().toString());
       // String prettyJsonString = gson.toJson(je);
       // System.out.println(prettyJsonString);

        //create kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();


        //loop to send data to kafka

        for (int i = 0; i < 5; i++) {
            String message = response.getBody().toString();
            producer.send(new ProducerRecord<String, String>("covid_data", null, message), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        //logger.error("Eish something broke china");
                        System.out.println("Eish something broke");
                    }
                }
            });
            Thread.sleep(1000);
        }

    }

    public KafkaProducer<String, String> createKafkaProducer() {

        String bootstrapServers = "127.0.0.1:9092";

        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;



    }


}
