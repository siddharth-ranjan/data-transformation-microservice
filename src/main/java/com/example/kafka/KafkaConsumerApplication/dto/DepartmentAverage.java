package com.example.kafka.KafkaConsumerApplication.dto;

public class DepartmentAverage {

    private String department;
    private double average_salary;

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public double getAverage_salary() {
        return average_salary;
    }

    public void setAverage_salary(double average_salary) {
        this.average_salary = average_salary;
    }
}
