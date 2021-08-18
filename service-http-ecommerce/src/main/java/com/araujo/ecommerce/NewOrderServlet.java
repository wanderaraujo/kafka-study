package com.araujo.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class NewOrderServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
	private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

	@Override
	public void destroy() {
		super.destroy();
		orderDispatcher.close();
		emailDispatcher.close();
	}


	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		try {

			var userEmail = req.getParameter("email");
			var amount = new BigDecimal(req.getParameter("amount"));
			var orderId = UUID.randomUUID().toString();
			var order = new Order(orderId, amount, userEmail);

			orderDispatcher.send("ECOMMERCE_NEW_ORDER", userEmail, order);

			var email = "Obrigado, estamos processando sua compra";
			emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userEmail, email);

			System.out.println("Processo da nova compra terminado");
			resp.setStatus(HttpServletResponse.SC_OK);
			resp.getWriter().println("Nova compra recebida");
		} catch (InterruptedException e) {
			throw new ServletException(e);
		} catch (ExecutionException e) {
			throw new ServletException(e);
		}

	}

}
