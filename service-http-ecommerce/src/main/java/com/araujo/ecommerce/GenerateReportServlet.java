package com.araujo.ecommerce;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class GenerateReportServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	private final KafkaDispatcher<String> batchDispatcher = new KafkaDispatcher<>();

	@Override
	public void destroy() {
		super.destroy();
		batchDispatcher.close();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		try {

			batchDispatcher.send("SEND_MESSAGE_TO_ALL_USERS", "USER_GENERATE_READING_REPORT", "USER_GENERATE_READING_REPORT");

			System.out.println("Gerando relatorios para vários usuários");
			resp.setStatus(HttpServletResponse.SC_OK);
			resp.getWriter().println("Geração de relatórios recebida");
		} catch (InterruptedException e) {
			throw new ServletException(e);
		} catch (ExecutionException e) {
			throw new ServletException(e);
		}

	}

}
