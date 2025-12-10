package by.antohakon.vetclinicorchestrator.event.message;

import by.antohakon.vetclinicorchestrator.entity.OutBox;
import by.antohakon.vetclinicorchestrator.repository.OutBoxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class ScheduledPublisher {

    private final OutBoxRepository outBoxRepository;
    private final KafkaSender kafkaSender;

    @Scheduled(fixedRate = 20000)
    @Transactional
    public void publishMessages() {
        log.info("Orchestrator publishMessage");

        List<OutBox> outBoxes = outBoxRepository.findAll();

        for (OutBox outBox : outBoxes) {
            kafkaSender.sendMessage(outBox);
        }
    }

}
