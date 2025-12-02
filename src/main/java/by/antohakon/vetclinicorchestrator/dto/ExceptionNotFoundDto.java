package by.antohakon.vetclinicorchestrator.dto;

import java.util.UUID;

public record ExceptionNotFoundDto(String errorMessage, UUID visitId) {}
