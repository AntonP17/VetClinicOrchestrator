package by.antohakon.vetclinicorchestrator.event.exceptionResponse;

import by.antohakon.vetclinicorchestrator.dto.ExceptionNotFoundDto;
import by.antohakon.vetclinicorchestrator.dto.VisitStatusEventDto;
import by.antohakon.vetclinicorchestrator.event.Status;
import by.antohakon.vetclinicorchestrator.event.message.KafkaSender;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.json.JsonParseException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
@RequiredArgsConstructor
@Slf4j
public class ErrorHandlingService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final KafkaSender kafkaSender;

    @Value("${kafka.topic.comand.delete}")
    private String deleteClientStatusTopic;

    @KafkaListener(
            topics = "${kafka.topic.exceptions}",
            groupId = "${kafka.group.response}",
            containerFactory = "orchestratorKafkaListenerContainerFactory"
    )
    public void handleError(String message) {
        try {
            ExceptionNotFoundDto exceptionNotFoundDto = objectMapper.readValue(message, ExceptionNotFoundDto.class);
            log.error("Received exception: {}", exceptionNotFoundDto.errorMessage());

            log.info("try send message to Visits");
            kafkaTemplate.send(deleteClientStatusTopic, message)
                    .whenComplete((result, ex) -> kafkaSender.processResult(ex));
            log.info("Sent message to deleteClientStatusTopic");

            log.info("try send messege Analitic");
            VisitStatusEventDto visitStatusEventDto = VisitStatusEventDto.builder()
                    .visitId(exceptionNotFoundDto.visitId())
                    .status(Status.FAILED)
                    .comment(exceptionNotFoundDto.errorMessage())
                    .build();

            String jsonAnalytics = objectMapper.writeValueAsString(visitStatusEventDto);
            kafkaSender.sendToAnalytics(jsonAnalytics, visitStatusEventDto.visitId().toString());
            log.info("sucesses send message Analitic: {}", jsonAnalytics);

        } catch (JsonParseException e) {
            log.error("Failed to deserialize error message: {}", e.getMessage());
        }
    }

}
