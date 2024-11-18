package spring_boot.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import spring_boot.dto.TaskDto;
import spring_boot.entity.Task;
import spring_boot.service.TaskService;

@RestController
@RequestMapping("/tasks")
public class TaskController {
    private TaskService taskService;

    @Autowired
    public TaskController(TaskService taskService) {
        this.taskService = taskService;
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<Task> createTask(@RequestBody TaskDto taskDto) {
        Task task = taskService.createTask(taskDto.getTitle(), taskDto.isCompleted());
        return ResponseEntity.status(HttpStatus.CREATED).body(task);
    }

    @RequestMapping(method = RequestMethod.PUT)
    public ResponseEntity<Task> updateTask(@RequestBody TaskDto taskDto) {
        Task task = taskService.updateTask(taskDto.getId(), taskDto.getTitle(), taskDto.isCompleted());
        return ResponseEntity.ok(task);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public ResponseEntity<Task> getTaskById(@PathVariable("id") Long id) {
        Task task = taskService.getTaskById(id)
                .orElseThrow(RuntimeException::new);
        return ResponseEntity.ok(task);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    public void deleteTaskById(@PathVariable("id") Long id) {
        taskService.deleteTaskById(id);
    }

}
