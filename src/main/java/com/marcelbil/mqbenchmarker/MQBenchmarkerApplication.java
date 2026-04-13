package com.marcelbil.mqbenchmarker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(excludeName = {
    "org.springframework.boot.autoconfigure.jms.artemis.ArtemisAutoConfiguration",
    "org.springframework.boot.autoconfigure.jms.JmsAutoConfiguration"
})
public class MQBenchmarkerApplication {
    public static void main(String[] args) {
        SpringApplication.run(MQBenchmarkerApplication.class, args);
    }
}