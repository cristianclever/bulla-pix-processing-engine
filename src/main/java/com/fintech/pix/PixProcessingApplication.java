package com.fintech.pix;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class PixProcessingApplication {

    public static void main(String[] args) {
        SpringApplication.run(PixProcessingApplication.class, args);
    }


}
