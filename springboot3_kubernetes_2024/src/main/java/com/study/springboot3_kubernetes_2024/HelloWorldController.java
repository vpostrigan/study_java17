package com.study.springboot3_kubernetes_2024;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloWorldController {

    @RequestMapping("/")
    public String helloWorld() {
        return "Hello World from " + this.getClass().getSimpleName();
    }

    @GetMapping("/home")
    public String Jetbrains() {
        return "Jetbrains from " + this.getClass().getSimpleName();
    }

    @GetMapping("/student")
    public StudentInformation getStudent() {
        return new StudentInformation(1, "John", "Beatles");
    }

}
