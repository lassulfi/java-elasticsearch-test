package br.com.github.lassulfi.respository.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.UUID;

import org.elasticsearch.action.index.IndexResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import br.com.github.lassulfi.repository.ElasticsearchRepository;
import br.com.github.lassulfi.repository.impl.ElasticsearchRepositoryImpl;

public class ElasticsearchRepositoryImplTest {

	private ElasticsearchRepository repo;
	
	private JsonNode jsonObject;
	
	private static final String ID = UUID.randomUUID().toString();
	private static final String INDEX = "likes";
	private static final String TYPE = "like";
	
	@Before
	public void setup() throws Exception {
		repo = new ElasticsearchRepositoryImpl();
		
		jsonObject = this.getJsonObject();
	}
	
	@Test
	public void testInsertObject() {
		IndexResponse response = repo.insert(INDEX, TYPE, ID, jsonObject);
		
		assertThat(response.getId(), is(ID));		
	}
		
	@After
	public void tearDown() {
		this.repo.deleteById(INDEX, TYPE, ID);
	}
	
	private JsonNode getJsonObject() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		
		String jsonString = "{\"fornecedor_id\": \"5200\",\n" +
	            "                    \"users_likes\": [\n" +
	            "                        {\n" +
	            "                            \"user_id\": \"1\"\n" +
	            "                        },\n" +
	            "                        {\n" +
	            "                            \"user_id\": \"3\"\n" +
	            "                        },\n" +
	            "                        {\n" +
	            "                            \"user_id\": \"5\"\n" +
	            "                        },\n" +
	            "                        {\n" +
	            "                            \"user_id\": \"10\"\n" +
	            "                        }\n" +
	            "                    ],\n" +
	            "                    \"users_dislikes\": [\n" +
	            "                        {\n" +
	            "                            \"user_id\": \"30\"\n" +
	            "                        },\n" +
	            "                        {\n" +
	            "                            \"user_id\": \"9\"\n" +
	            "                        },\n" +
	            "                        {\n" +
	            "                            \"user_id\": \"4\"\n" +
	            "                        }\n" +
	            "                    ]\n" +
	            "}";
		
		return mapper.readTree(jsonString);
	}
}
