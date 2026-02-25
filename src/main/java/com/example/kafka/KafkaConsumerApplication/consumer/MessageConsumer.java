package com.example.kafka.KafkaConsumerApplication.consumer;

import com.example.kafka.KafkaConsumerApplication.dto.DepartmentAverage;
import com.example.kafka.KafkaConsumerApplication.dto.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MessageConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "average-salary-topic", groupId = "csv-processing-group")
    public void consumeDepartmentAverage(String message) {
        try {
            DepartmentAverage avg =
                    objectMapper.readValue(message, DepartmentAverage.class);

            System.out.println("\n=========== DEPARTMENT AVERAGE ===========");
            System.out.println("Department      : " + avg.getDepartment());
            System.out.printf("Average Salary  : %.2f%n", avg.getAverage_salary());
            System.out.println("==========================================\n");

        } catch (Exception e) {
            System.err.println("Error processing department message: " + e.getMessage());
        }
    }

    @KafkaListener(topics = "high-salary-topic", groupId = "csv-processing-group")
    public void consumeHighSalaryEmployees(String message) {

        try {
            Employee emp =
                    objectMapper.readValue(message, Employee.class);

            System.out.println("\n=========== HIGH SALARY EMPLOYEE ===========");
            System.out.println("ID          : " + emp.getId());
            System.out.println("Name        : " + emp.getName());
            System.out.println("Department  : " + emp.getDepartment());
            System.out.printf("Salary      : %.2f%n", emp.getSalary());
            System.out.println("============================================\n");

        } catch (Exception e) {
            System.err.println("Error processing employee message: " + e.getMessage());
        }
    }

}
