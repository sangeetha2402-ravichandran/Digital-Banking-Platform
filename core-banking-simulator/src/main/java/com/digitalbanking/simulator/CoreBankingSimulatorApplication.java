package com.digitalbanking.simulator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jms.annotation.EnableJms;

@SpringBootApplication
@EnableJms
public class CoreBankingSimulatorApplication {

	public static void main(String[] args) {
		SpringApplication.run(
				CoreBankingSimulatorApplication.class,
				args
		);
	}
}