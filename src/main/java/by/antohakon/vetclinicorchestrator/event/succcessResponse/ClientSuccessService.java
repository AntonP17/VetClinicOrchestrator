package by.antohakon.vetclinicorchestrator.event.succcessResponse;

import by.antohakon.vetclinicorchestrator.dto.AnimalAndOwnerEvent;
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
public class ClientSuccessService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final KafkaSender kafkaSender;

    @Value("${kafka.topic.comand.save.client}")
    private String changeStatusClientTopic;

    @KafkaListener(
            topics = "${kafka.topic.successful.client.response}",
            groupId = "${kafka.group.response}",
            containerFactory = "orchestratorKafkaListenerContainerFactory"
    )
    public void handleClientSuccess(String message) {
        try {
            log.info("Take message {}", message);
            AnimalAndOwnerEvent animalAndOwnerEvent = objectMapper.readValue(message, AnimalAndOwnerEvent.class);
            log.info("after parsing {}", animalAndOwnerEvent.toString());

            log.info("Animal owners response: {}", animalAndOwnerEvent.toString());

            String jsonChangeClientStatus = objectMapper.writeValueAsString(animalAndOwnerEvent);
            kafkaTemplate.send(changeStatusClientTopic, jsonChangeClientStatus)
                    .whenComplete((result, ex) -> kafkaSender.processResult(ex));
            log.info("successful send message changeStatusTopic: {}", jsonChangeClientStatus);

            log.info("try send messege Analitic");
            VisitStatusEventDto visitStatusEventDto = VisitStatusEventDto.builder()
                    .visitId(animalAndOwnerEvent.visitId())
                    .status(Status.SUCCESS)
                    .comment("ACCEPTED")
                    .build();

            String jsonAnalytics = objectMapper.writeValueAsString(visitStatusEventDto);
            kafkaSender.sendToAnalytics(jsonAnalytics, visitStatusEventDto.visitId().toString());
            log.info("Sent successful analytic message: {}", jsonAnalytics);

        } catch (JsonParseException e) {
            log.error("Failed to deserialize client success message: {}", e.getMessage());
        }
    }

}
