package by.antohakon.vetclinicorchestrator.exceptions;

public class KafkaSendMessageException extends RuntimeException {
    public KafkaSendMessageException(String message) {
        super(message);
    }
}
