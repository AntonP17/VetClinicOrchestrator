package by.antohakon.vetclinicorchestrator.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "outbox_table")
@Getter
@Setter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OutBox {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long messageId;

    @Column(nullable = false, length = 1000)
    private String payload;

    @Column(nullable = false)
    private String type;

    @Column(nullable = false)
    private LocalDateTime createdAt;

}
