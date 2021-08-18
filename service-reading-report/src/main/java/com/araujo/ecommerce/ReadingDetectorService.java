package com.araujo.ecommerce;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ReadingDetectorService {

	private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

	public static void main(String[] args) {
		var reportService = new ReadingDetectorService();
		try (var service = new KafkaService<User>(ReadingDetectorService.class.getSimpleName(),
				"USER_GENERATE_READING_REPORT", reportService::parse, User.class, Map.of())) {
			service.run();
		}

	}

	private void parse(ConsumerRecord<String, User> record) throws IOException {

		System.out.println("---------------------------------------------");
		System.out.println("CONSUMER:::processando relatório para usuário: " + record.value());

		var user = record.value();
		var target = new File(user.getReportPath());
		IO.copyTo(SOURCE, target);
		IO.append(target, "Criado para: " + user.getUuid());

		System.out.println("Arquivo criado: " + target.getAbsolutePath());

	}
}
