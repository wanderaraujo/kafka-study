package com.araujo.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		try (var orderDispatcher = new KafkaDispatcher<Order>()) {
			try (var emailDispatcher = new KafkaDispatcher<String>()) {
				var userEmail = Math.random() + "@email.com";

				for (int i = 0; i < 10; i++) {

//					var userId = UUID.randomUUID().toString();
					var orderId = UUID.randomUUID().toString();
					var amount = new BigDecimal(Math.random() * 5000 + 1);
					var order = new Order( orderId, amount, userEmail);

					orderDispatcher.send("ECOMMERCE_NEW_ORDER", userEmail, order);

					var email = "Obrigado, estamos processando sua compra";
//					var email = new Email("Obrigado", "estamos processando sua compra");
					emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userEmail, email);

				}
			}

		}

	}

}
