package spring_boot.service;

import spring_boot.entity.Task;

import java.util.Optional;

public interface TaskService {
    Task createTask(String title, boolean completed);

    Task updateTask(Long id, String title, boolean completed);

    Optional<Task> getTaskById(Long id);

    void deleteTaskById(Long id);
}
