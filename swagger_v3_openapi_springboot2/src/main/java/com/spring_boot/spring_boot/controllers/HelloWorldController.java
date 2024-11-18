package com.spring_boot.spring_boot.controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * https://www.youtube.com/watch?v=5kOGdZmpSDI
 * Creating a Spring Boot "Hello World" Application with IntelliJ IDEA (2021)
 */
@RestController
public class HelloWorldController {

    @RequestMapping
    public String helloWorld() {
        return "Hello World from Spring Boot";
    }

    @RequestMapping("/goodbye")
    public String goodbye() {
        return "Goodbye from Spring Boot";
    }

}
