package by.antohakon.vetclinicorchestrator.repository;

import by.antohakon.vetclinicorchestrator.entity.OutBox;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OutBoxRepository extends JpaRepository<OutBox, Long> {
}
