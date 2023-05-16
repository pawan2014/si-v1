package com.example.pk.spring.integration.v1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class SpringIntegrationV1Application {

	public static void main(String[] args) {
		SpringApplication.run(SpringIntegrationV1Application.class, args);
	}

}
