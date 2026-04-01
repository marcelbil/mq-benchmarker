package com.marcelbil.mqbenchmarker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jms.JmsAutoConfiguration;
import org.springframework.boot.autoconfigure.jms.artemis.ArtemisAutoConfiguration;
import org.springframework.jms.annotation.EnableJms;

// Schakel de automatische Spring Boot connecties uit, wij doen dit dynamisch via de UI!
@SpringBootApplication(exclude = {ArtemisAutoConfiguration.class, JmsAutoConfiguration.class})
@EnableJms
public class ArtemisBenchmarkApplication {
    public static void main(String[] args) {
        SpringApplication.run(ArtemisBenchmarkApplication.class, args);
    }
}