package com.araujo.ecommerce;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

	public static void main(String[] args) {
		var fraudService = new FraudDetectorService();
		try (var service = new KafkaService<Order>(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
				fraudService::parse, Order.class, Map.of())) {
			service.run();
		}

	}

	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>();

	private void parse(ConsumerRecord<String, Order> record) throws InterruptedException, ExecutionException {

		System.out.println("---------------------------------------------");
		System.out.println("CONSUMER:::processando nova compra, verificando fraude");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());

		try {
			Thread.sleep(500);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		var order  = record.value();
		
		if(isFraud(order)) {
			// se valor for acima de 4500 cosirar como fraude
			System.out.println("Compra é uma fraude!!!" + order.toString());
			orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
		}else {
			orderDispatcher.send("ECOMMERCE_ORDER_APPOVED", order.getEmail(), order);
			System.out.println("Compra Aprovada!!!" + order.toString());
		}

		System.out.println("CONSUMER:::ordem processada");

	}

	private boolean isFraud(Order order) {
		return order.getAmount().compareTo(new BigDecimal("4500")) >0 ;
	}

}
