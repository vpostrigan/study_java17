package spring_boot.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import spring_boot.entity.Task;

@Repository
public interface TaskRepository extends JpaRepository<Task, Long> {
}
