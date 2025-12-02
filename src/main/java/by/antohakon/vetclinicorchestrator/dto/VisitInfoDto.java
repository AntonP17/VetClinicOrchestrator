package by.antohakon.vetclinicorchestrator.dto;

import lombok.Builder;

import java.time.LocalDateTime;
import java.util.UUID;

@Builder
public record VisitInfoDto(UUID visitId, UUID ownerId, UUID doctorId, UUID animalId, String reasonRequest, LocalDateTime visitDate) {
}
