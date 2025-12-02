package by.antohakon.vetclinicorchestrator.dto;

import by.antohakon.vetclinicorchestrator.event.Status;
import lombok.Builder;

import java.util.UUID;

@Builder
public record VisitStatusEventDto(UUID visitId,
                                  Status status,
                                  String comment) {
}
