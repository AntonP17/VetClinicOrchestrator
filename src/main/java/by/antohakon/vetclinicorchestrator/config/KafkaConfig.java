package by.antohakon.vetclinicorchestrator.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60000);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> orchestratorKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConcurrency(3);
        factory.setConsumerFactory(orchestratorGroupConsumerFactory());

        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> orchestratorGroupConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, "orchestratorGroup");
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public NewTopic clientsTopic() {
        return TopicBuilder.name("animals_owners")
                .partitions(3)
                .replicas(2)
                .build();
    }

    @Bean
    public NewTopic visitsTopic() {
        return TopicBuilder.name("visit-topic-create")
                .partitions(3)
                .replicas(2)
                .build();
    }

    @Bean
    public NewTopic changeStatusVisitTopic() {
        return TopicBuilder.name("change-status-visit")
                .partitions(3)
                .replicas(2)
                .build();
    }

    @Bean
    public NewTopic doctorDeleteStatusTopic() {
        return TopicBuilder.name("delete-status-doctor")
                .partitions(3)
                .replicas(2)
                .build();
    }

    @Bean
    public NewTopic clientDeleteStatusTopic() {
        return TopicBuilder.name("delete-status-client")
                .partitions(3)
                .replicas(2)
                .build();
    }

    @Bean
    public NewTopic doctorsTopic() {
        return TopicBuilder.name("doctors")
                .partitions(3)
                .replicas(2)
                .build();
    }

    @Bean
    public NewTopic exceptionTopic() {
        return TopicBuilder.name("exceptions")
                .partitions(3)
                .replicas(2)
                .build();
    }

    @Bean
    public NewTopic analiticTopic() {
        return TopicBuilder.name("analytics")
                .partitions(3)
                .replicas(2)
                .build();
    }





}
