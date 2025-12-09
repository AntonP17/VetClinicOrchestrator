package by.antohakon.vetclinicorchestrator.event;

import by.antohakon.vetclinicorchestrator.dto.*;
import by.antohakon.vetclinicorchestrator.entity.OutBox;
import by.antohakon.vetclinicorchestrator.exceptions.KafkaSendMessageException;
import by.antohakon.vetclinicorchestrator.repository.OutBoxRepository;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.json.JsonParseException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class Orchestrator {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OutBoxRepository outBoxRepository;

    @Value("${kafka.topic.comand.save.client}")
    private String changeStatusClientTopic;
    @Value("${kafka.topic.comand.save.doctor}")
    private String changeStatusDOctorTopic;
    @Value("${kafka.topic.clients}")
    private String clientsTopic;
    @Value("${kafka.topic.doctors}")
    private String doctorTopic;
    @Value("${kafka.topic.analytics}")
    private String analyticsTopic;
    @Value("${kafka.topic.comand.delete}")
    private String deleteClientStatusTopic;

    // начало цепочки принимаем сообщение нового визита
    @Transactional
    @KafkaListener(
            topics = "${kafka.topic.new.visit}",
            groupId = "${kafka.group.new.visit}",
            containerFactory = "orchestratorKafkaListenerContainerFactory"
    )
    public void listenCreateVisitMessage(String message) {
        log.info("Orchestrator listemMessage = {}", message);

        OutBox outBox = OutBox.builder()
                .payload(message)
                .type("new visit")
                .createdAt(LocalDateTime.now())
                .build();

        outBoxRepository.save(outBox);


    }

    @Scheduled(fixedRate = 20000)
    @Transactional
    public void publishMessage() {
        log.info("Orchestrator publishMessage");

        List<OutBox> outBoxList = outBoxRepository.findAll();

        for (OutBox outBox : outBoxList) {
            sendMessage(outBox);
        }

    }

    // отправляем сообщение для поиска врача, владельца с животным
    private void sendMessage(OutBox outBox) {

        try {
            VisitInfoDto visitInfoDto = objectMapper.readValue(outBox.getPayload(), VisitInfoDto.class);
            String json = objectMapper.writeValueAsString(visitInfoDto);
            log.info("try send message : {}", json);
            VisitStatusEventDto visitStatusEventDto = VisitStatusEventDto.builder()
                    .visitId(visitInfoDto.visitId())
                    .status(Status.PROCESSING)
                    .comment("create new visit")
                    .build();

            String jsonAnalitics = objectMapper.writeValueAsString(visitStatusEventDto);

            // можно вывести три этих кода в один метод отпарвки!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            kafkaTemplate.send(clientsTopic, visitInfoDto.visitId().toString(), json)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to send message to Kafka (animals_owners)", ex);
                            throw new KafkaSendMessageException("ошибка отправки сообщения в animals_owners");
                        } else {
                            log.info("Message sent to Kafka (animals_owners) successfully");
                        }
                    });

            kafkaTemplate.send(doctorTopic, visitInfoDto.visitId().toString(), json)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to send message to Kafka (doctors)", ex);
                            throw new KafkaSendMessageException("ошибка отправки сообщения в (doctors)");
                        } else {
                            log.info("Message sent to Kafka (doctors) successfully");
                        }
                    });


            kafkaTemplate.send(analyticsTopic, visitInfoDto.visitId().toString(), jsonAnalitics)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to send message to Kafka (analytics)", ex);
                            throw new KafkaSendMessageException("ошибка отправки сообщения в (analytics)");
                        } else {
                            log.info("Message sent to Kafka (analytics) successfully");
                        }
                    });

            // проверить в логах удаление и чтобы номр рабоатло
            outBoxRepository.delete(outBox);

        } catch (JsonParseException e) {
            log.error("Failed to serialize order: {}", e.getMessage());
        }


    }

    // не найден клиент или врач
    @SneakyThrows
    @KafkaListener(
            topics = "${kafka.topic.exceptions}",
            groupId = "${kafka.group.response}",
            containerFactory = "orchestratorKafkaListenerContainerFactory"
    )
    public void listenClientsException(String messageexception) {

        ExceptionNotFoundDto exceptionNotFoundDto = objectMapper.readValue(messageexception, ExceptionNotFoundDto.class);
        log.error("Received exception: {}", exceptionNotFoundDto.errorMessage());

        log.info("try send message to Visits");
        kafkaTemplate.send(deleteClientStatusTopic, messageexception);
        log.info("Successful send message to deleteClientStatusTopic");

        log.info("try send messege Analitic");
        VisitStatusEventDto visitStatusEventDto = VisitStatusEventDto.builder()
                .visitId(exceptionNotFoundDto.visitId())
                .status(Status.FAILED)
                .comment(exceptionNotFoundDto.errorMessage())
                .build();

        String jsonAnalitics = objectMapper.writeValueAsString(visitStatusEventDto);
        kafkaTemplate.send(analyticsTopic, exceptionNotFoundDto.visitId().toString(), jsonAnalitics);
        log.info("sucesses send message Analitic: {}", jsonAnalitics);
    }

    // найден владелец с животным
    @KafkaListener(
            topics = "${kafka.topic.successful.client.response}",
            groupId = "${kafka.group.response}",
            containerFactory = "orchestratorKafkaListenerContainerFactory"
    )
    @Transactional(isolation = Isolation.REPEATABLE_READ)

    public void listenAnimalOwnersResponse(String message) {

        AnimalAndOwnerEvent animalAndOwnerEvent = null;
        try {
            log.info("Take message {}", message);
            animalAndOwnerEvent = objectMapper.readValue(message, AnimalAndOwnerEvent.class);
            log.info("after parsing {}", animalAndOwnerEvent.toString());
        } catch (JsonParseException e) {
            log.error("Failed to parse order from JSON: {}", message, e);
        }
        log.info("AnimalOwners response: {}", animalAndOwnerEvent.toString());

        String jsonChangeClientStatus = objectMapper.writeValueAsString(animalAndOwnerEvent);
        kafkaTemplate.send(changeStatusClientTopic, jsonChangeClientStatus);
        log.info("successful send message changeStatusTopic: {}", jsonChangeClientStatus);

        log.info("try send messege Analitic");
        VisitStatusEventDto visitStatusEventDto = VisitStatusEventDto.builder()
                .visitId(animalAndOwnerEvent.visitId())
                .status(Status.SUCCESS)
                .comment("ACEPTED")
                .build();

        String jsonAnalitics = objectMapper.writeValueAsString(visitStatusEventDto);
        kafkaTemplate.send(analyticsTopic, animalAndOwnerEvent.visitId().toString(), jsonAnalitics);
        log.info("sucesses send message Analitic: {}", jsonAnalitics);
    }

    // найден врач
    @KafkaListener(
            topics = "${kafka.topic.successful.doctor.response}",
            groupId = "${kafka.group.response}",
            containerFactory = "orchestratorKafkaListenerContainerFactory"
    )
    public void listenDoctorsResponse(String message) {

        EmployeEvent employeEvent = null;
        try {
            log.info("Take message {}", message);
            employeEvent = objectMapper.readValue(message, EmployeEvent.class);
            log.info("after parsing {}", employeEvent.toString());
        } catch (JsonParseException e) {
            log.error("Failed to parse order from JSON: {}", message, e);
        }

        log.info("Doctors response: {}", employeEvent.toString());
        String jsonChangeDoctorStatus = objectMapper.writeValueAsString(employeEvent);
        kafkaTemplate.send(changeStatusDOctorTopic, jsonChangeDoctorStatus);
        log.info("sucesses send message changeStatusDOctorStatus: {}", jsonChangeDoctorStatus);


        log.info("try send messege Analitic");
        VisitStatusEventDto visitStatusEventDto = VisitStatusEventDto.builder()
                .visitId(employeEvent.visitId())
                .status(Status.SUCCESS)
                .comment("ACEPTED")
                .build();

        String jsonAnalitics = objectMapper.writeValueAsString(visitStatusEventDto);
        kafkaTemplate.send(analyticsTopic, employeEvent.visitId().toString(), jsonAnalitics);
        log.info("sucesses send message Analitic: {}", jsonAnalitics);
    }


}
