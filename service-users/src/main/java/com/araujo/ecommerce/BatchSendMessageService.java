package com.araujo.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class BatchSendMessageService {

	private final Connection connection;

	BatchSendMessageService() throws SQLException {
		String url = "jdbc:sqlite:users_database.db";
		this.connection = DriverManager.getConnection(url);
		connection.createStatement()
				.execute("create table  IF NOT EXISTS Users (uuid varchar(200) primary key, email varchar(200))");
	}

	public static void main(String[] args) throws SQLException {
		var batchService = new BatchSendMessageService();
		try (var service = new KafkaService(BatchSendMessageService.class.getSimpleName(), "SEND_MESSAGE_TO_ALL_USERS",
				batchService::parse, String.class, Map.of())) {
			service.run();
		}

	}

	private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

	private void parse(ConsumerRecord<String, String> record)
			throws InterruptedException, ExecutionException, SQLException {

		System.out.println("---------------------------------------------");
		System.out.println("CONSUMER:::processando nova batch");
		System.out.println("TOPICO: " + record.value());

		for (User user : getAllUsers()) {
			userDispatcher.send(record.value(), user.getUuid(), user);
		}

	}

	private List<User> getAllUsers() throws SQLException {
		var select = connection.prepareStatement("select uuid from Users").executeQuery();

		List<User> users = new ArrayList<>();

		while (select.next()) {
			users.add(new User(select.getString(1)));
		}

		return users;
	}

}
