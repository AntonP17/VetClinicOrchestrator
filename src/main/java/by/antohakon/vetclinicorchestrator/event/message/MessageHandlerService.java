package by.antohakon.vetclinicorchestrator.event.message;

import by.antohakon.vetclinicorchestrator.entity.OutBox;
import by.antohakon.vetclinicorchestrator.repository.OutBoxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class MessageHandlerService {

    private final OutBoxRepository outBoxRepository;

    @KafkaListener(
            topics = "${kafka.topic.new.visit}",
            groupId = "${kafka.group.new.visit}",
            containerFactory = "orchestratorKafkaListenerContainerFactory"
    )
    @Transactional
    public void handleNewVisitMessage(String message) {
        log.info("Orchestrator listening new visit message: {}", message);

        OutBox outBox = OutBox.builder()
                .payload(message)
                .type("new visit")
                .createdAt(LocalDateTime.now())
                .build();

        outBoxRepository.save(outBox);
    }

}
