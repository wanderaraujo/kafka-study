package com.araujo.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class CreateUserService {

	private final Connection connection;

	CreateUserService() throws SQLException {
		String url = "jdbc:sqlite:users_database.db";
		this.connection = DriverManager.getConnection(url);
		connection.createStatement()
				.execute("create table  IF NOT EXISTS Users (uuid varchar(200) primary key, email varchar(200))");
	}

	public static void main(String[] args) throws SQLException {
		var createUserService = new CreateUserService();
		try (var service = new KafkaService<Order>(CreateUserService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
				createUserService::parse, Order.class, Map.of())) {
			service.run();
		}

	}

	private void parse(ConsumerRecord<String, Order> record)
			throws InterruptedException, ExecutionException, SQLException {

		System.out.println("---------------------------------------------");
		System.out.println("CONSUMER:::processando nova compra, verificando novo usuário");
		System.out.println(record.value());
		var order = record.value();

		if (!isNewUser(order.getEmail())) {
			insertNewUser(order.getEmail(), order.getEmail());
		}

	}

	private void insertNewUser(String uuid, String email) throws SQLException {
		var insert = connection.prepareStatement("insert into Users (uuid, email) values (?,?)");
		insert.setString(1, UUID.randomUUID().toString());
		insert.setString(2, email);
		insert.execute();
		System.out.println("Criando Usuario uuid " + uuid + " com email " + email);
	}

	private boolean isNewUser(String email) throws SQLException {
		var select = connection.prepareStatement("select * from Users where email = ?");
		select.setString(1, email);
		var results = select.executeQuery();
		return results.next();
	}

}
