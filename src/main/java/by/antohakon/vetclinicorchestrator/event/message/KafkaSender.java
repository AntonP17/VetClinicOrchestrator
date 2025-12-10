package by.antohakon.vetclinicorchestrator.event.message;

import by.antohakon.vetclinicorchestrator.dto.VisitInfoDto;
import by.antohakon.vetclinicorchestrator.dto.VisitStatusEventDto;
import by.antohakon.vetclinicorchestrator.entity.OutBox;
import by.antohakon.vetclinicorchestrator.event.Status;
import by.antohakon.vetclinicorchestrator.exceptions.KafkaSendMessageException;
import by.antohakon.vetclinicorchestrator.repository.OutBoxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.json.JsonParseException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaSender {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OutBoxRepository outBoxRepository;

    @Value("${kafka.topic.clients}")
    private String clientsTopic;
    @Value("${kafka.topic.doctors}")
    private String doctorTopic;
    @Value("${kafka.topic.analytics}")
    private String analyticsTopic;

    public void sendMessage(OutBox outBox) {
        try {
            VisitInfoDto visitInfoDto = objectMapper.readValue(outBox.getPayload(), VisitInfoDto.class);
            String json = objectMapper.writeValueAsString(visitInfoDto);

            VisitStatusEventDto visitStatusEventDto = VisitStatusEventDto.builder()
                    .visitId(visitInfoDto.visitId())
                    .status(Status.PROCESSING)
                    .comment("create new visit")
                    .build();

            String jsonAnalytics = objectMapper.writeValueAsString(visitStatusEventDto);

            kafkaTemplate.send(clientsTopic, visitInfoDto.visitId().toString(), json)
                    .whenComplete((result, ex) -> processResult(ex));

            kafkaTemplate.send(doctorTopic, visitInfoDto.visitId().toString(), json)
                    .whenComplete((result, ex) -> processResult(ex));

            kafkaTemplate.send(analyticsTopic, visitInfoDto.visitId().toString(), jsonAnalytics)
                    .whenComplete((result, ex) -> processResult(ex));

            outBoxRepository.delete(outBox);

        } catch (JsonParseException e) {
            log.error("Failed to serialize order: {}", e.getMessage());
        }
    }

    public void sendToAnalytics(String payload, String partitionKey) {
        try {
            kafkaTemplate.send(analyticsTopic, partitionKey, payload)
                    .whenComplete((result, ex) -> processResult(ex));
            log.info("sucesses send message Analitic: {}", payload);
        } catch (JsonParseException e) {
            log.error("Failed to send message to Analytics topic.", e);
        }
    }

    public void processResult(Throwable ex) {
        if (ex != null) {
            log.error("Failed to send message to Kafka", ex);
            throw new KafkaSendMessageException("Ошибка отправки сообщения в Kafka");
        } else {
            log.info("Message sent to Kafka successfully");
        }
    }

}
