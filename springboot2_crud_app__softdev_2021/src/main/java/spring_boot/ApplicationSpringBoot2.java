package spring_boot;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import spring_boot.entity.Employee;
import spring_boot.repository.EmployeeRepository;

/*
@SpringBootApplication(scanBasePackages = {
        "spring_boot.entity",
        "spring_boot.repository",
        "spring_boot.service",
        "spring_boot.service.impl",
        "spring_boot.controller" })*/
@SpringBootApplication
public class ApplicationSpringBoot2 {

    public static void main(String[] args) {
        SpringApplication.run(ApplicationSpringBoot2.class, args);
    }


    // https://www.youtube.com/watch?v=wuX2ESOy-Ts

    @Bean
    public CommandLineRunner demo(EmployeeRepository repository) {
        return args -> {
            insertJavaAdvocates(repository);
            System.out.println(repository.findAll());

            System.out.println(repository.findEmployeeByLastNameContaining("2"));
        };
    }

    private void insertJavaAdvocates(EmployeeRepository r) {
        r.save(new Employee("firstName1", "lastName1"));
        r.save(new Employee("firstName2", "lastName2"));
        r.save(new Employee("firstName3", "lastName3"));
        r.save(new Employee("firstName4", "lastName4"));
    }

}
