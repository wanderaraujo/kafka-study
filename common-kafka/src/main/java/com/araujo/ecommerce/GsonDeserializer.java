package com.araujo.ecommerce;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonDeserializer<T> implements Deserializer<T> {

	public static final String TYPE_CONFIG = "com.araujo.ecommerce";
	private final Gson gson = new GsonBuilder().create();
	private Class<T> type;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		String typeName = String.valueOf(configs.get(TYPE_CONFIG));
		try {
			this.type = (Class<T>) Class.forName(typeName);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Tipo de classe nao existe para deserializacao");
		}
	}

	@Override
	public T deserialize(String topic, byte[] bytes) {
		return gson.fromJson(new String(bytes), type);
	}

}
