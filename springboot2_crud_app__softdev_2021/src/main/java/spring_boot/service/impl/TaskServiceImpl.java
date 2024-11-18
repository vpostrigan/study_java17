package spring_boot.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import spring_boot.entity.Task;
import spring_boot.repository.TaskRepository;
import spring_boot.service.TaskService;

import java.time.LocalDate;
import java.util.Optional;

@Service
public class TaskServiceImpl implements TaskService {
    private TaskRepository taskRepository;

    @Autowired
    public TaskServiceImpl(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    @Override
    public Task createTask(String title, boolean completed) {
        Task task = mapTask(title, completed);

        return taskRepository.save(task);
    }

    @Override
    public Task updateTask(Long id, String title, boolean completed) {
        Task task = mapTask(title, completed);
        task.setId(id);

        return taskRepository.save(task);
    }

    @Override
    public Optional<Task> getTaskById(Long id) {
        return taskRepository.findById(id);
    }

    @Override
    public void deleteTaskById(Long id) {
        taskRepository.deleteById(id);
    }

    private Task mapTask(String title, boolean completed) {
        Task task = new Task();
        task.setTitle(title);
        task.setCompleted(completed);
        task.setCreationDate(LocalDate.now());
        return task;
    }

}
