package com.araujo.ecommerce;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ReadingDetectorService {
		
	private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();
	
	public static void main(String[] args) {
		var reportService = new ReadingDetectorService();
		try (var service = new KafkaService<Order>(ReadingDetectorService.class.getSimpleName(), "USER_GENERATE_READING_REPORT",
				reportService::parse, Order.class, Map.of())) {
			service.run();
		}

	}

//	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>();

	private void parse(ConsumerRecord<String, User> record) throws InterruptedException, ExecutionException {

		System.out.println("---------------------------------------------");
		System.out.println("CONSUMER:::processando relatório para usuário: " + record.value());
		
		var user = record.value();
		var target = new File(user.getReportPath());
		
		IO.copyTo(SOURCE, ra)
		
		
		
	}
}

