package by.antohakon.vetclinicorchestrator.event.succcessResponse;

import by.antohakon.vetclinicorchestrator.dto.EmployeEvent;
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
public class DoctorSuccessService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final KafkaSender kafkaSender;

    @Value("${kafka.topic.comand.save.doctor}")
    private String changeStatusDOctorTopic;

    @KafkaListener(
            topics = "${kafka.topic.successful.doctor.response}",
            groupId = "${kafka.group.response}",
            containerFactory = "orchestratorKafkaListenerContainerFactory"
    )
    public void handleDoctorSuccess(String message) {
        try {
            log.info("Take message {}", message);
            EmployeEvent employeEvent = objectMapper.readValue(message, EmployeEvent.class);
            log.info("after parsing {}", employeEvent.toString());

            log.info("Doctors response: {}", employeEvent.toString());

            String jsonChangeDoctorStatus = objectMapper.writeValueAsString(employeEvent);
            kafkaTemplate.send(changeStatusDOctorTopic, jsonChangeDoctorStatus)
                    .whenComplete((result, ex) -> kafkaSender.processResult(ex));
            log.info("sucesses send message changeStatusDOctorStatus: {}", jsonChangeDoctorStatus);

            log.info("try send messege Analitic");
            VisitStatusEventDto visitStatusEventDto = VisitStatusEventDto.builder()
                    .visitId(employeEvent.visitId())
                    .status(Status.SUCCESS)
                    .comment("ACEPTED")
                    .build();

            String jsonAnalytics = objectMapper.writeValueAsString(visitStatusEventDto);
            kafkaSender.sendToAnalytics(jsonAnalytics, visitStatusEventDto.visitId().toString());
            log.info("Sent successful analytic message: {}", jsonAnalytics);

        } catch (JsonParseException e) {
            log.error("Failed to deserialize doctor success message: {}", e.getMessage());
        }
    }

}
