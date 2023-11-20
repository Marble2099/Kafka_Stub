import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Kafka_Stub {
    private static final Logger log = LoggerFactory.getLogger(Kafka_Stub.class);

    public static void main(String[] args) {
        String CONSUMER_TOPIC = "";

        // Задаем конфигурационные параметры для консумера
        Properties consumerProps = new Properties();
        try {
            consumerProps.load(new FileReader("src/main/resources/Consumer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Задаем конфигурационные файлы для продюсера
        String PRODUCER_TOPIC = "";

        Properties producerProps = new Properties();
        try {
            producerProps.load(new FileReader("src/main/resources/Producer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {

            // Подключаем консумера к топику, из которого будем вычитывать сообщения
            consumer.subscribe(Arrays.asList(CONSUMER_TOPIC));

            // Запускаем бесконечный цикл в котором мы слушаем топик и выводим сообщения из него
            while (true) {
                try {
                    // Метод poll проверяет и ждет поступления новых сообщений для подключенного топика.
                    // Если нет сообщений в течение периода, указанного в аргументе, он возвращает пустой список
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    //log.info("Consumer started!");
                    if (!records.isEmpty()) {
                        for (ConsumerRecord<String, String> record : records) {
//                            log.info(record.value());

                            // Форматируем входящее сообщение в формат JSON парсим его
                            String jsonString = record.value();
                            Gson gson = new Gson();
                            JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);
                            String requestId = jsonObject.get("requestId").getAsString();

                            // Создаем новое сообщение для другого топика
                            String message = "{\"requestId\":\"" + requestId + "\",\"error\": \"false}\"}";

                            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(PRODUCER_TOPIC, message);
                            //log.info(producerRecord.value());
                            System.out.println(producerRecord.value());

                            // Посылаем новое сообщение в другой топик
                            producer.send(producerRecord);
//                            log.info("key: " + record.key() + ", Value: " + record.value());
//                            log.info("Partition: " + record.pertition() + ", Offset: " + record.offset());
                        }
                    }
                } catch (ClassCastException e) {
                    System.out.println("Not a JSON format: " + e.getMessage()); // Если приходит сообщение не в виде JSON
                } catch (NullPointerException e) {
                    System.out.println("Parameter requestId is null!"); // Если нет параметра с именем requestId
                }
            }
        }
    }
}
