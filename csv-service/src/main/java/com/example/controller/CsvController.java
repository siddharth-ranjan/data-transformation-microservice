package com.example.controller;

import com.example.service.CsvService;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CsvController {
    private final CsvService csvService;

    public CsvController(CsvService csvService) {
        this.csvService = csvService;
    }

    @GetMapping("/download/users")
    public ResponseEntity<InputStreamResource> downloadCsv() {

        InputStreamResource file = new InputStreamResource(csvService.fetchCsvFromS3());

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=emp_data.csv")
                .contentType(MediaType.parseMediaType("text/csv"))
                .body(file);
    }
}