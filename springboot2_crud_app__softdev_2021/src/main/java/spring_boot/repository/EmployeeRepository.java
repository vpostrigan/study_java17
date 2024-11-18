package spring_boot.repository;

import org.springframework.data.repository.CrudRepository;
import spring_boot.entity.Employee;

import java.util.List;

public interface EmployeeRepository extends CrudRepository<Employee, Long> {
    List<Employee> findEmployeeByLastNameContaining(String str);
}
