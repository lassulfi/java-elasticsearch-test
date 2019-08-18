package br.com.github.lassulfi.respository.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
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
	
	private static final String FORNECEDOR_ID = "6500";
	
	@Before
	public void setup() throws Exception {
		repo = new ElasticsearchRepositoryImpl();
		
		jsonObject = this.getJsonObject(FORNECEDOR_ID);
	}
	
	@Test
	public void testInsertObject() {
		IndexResponse response = repo.insert(INDEX, TYPE, ID, jsonObject);
		
		assertThat(response.getId(), is(ID));		
	}
	
	@Test
	public void testFindById() {
		repo.insert(INDEX, TYPE, ID, jsonObject);
		
		JsonNode jsonObject = repo.getById(INDEX, TYPE, ID);
		
		assertThat(jsonObject.get("fornecedor_id").asText(), is(FORNECEDOR_ID));
	}
	
	@Test
	public void testUpsertNewObject() {
		UpdateResponse response = repo.update(INDEX, TYPE, ID, jsonObject);
		
		assertThat(response.getId(), is(ID));
	}
	
	@Test
	public void testUpdateExistingObject() throws Exception {
		repo.insert(INDEX, TYPE, ID, jsonObject);
		
		String newFornecedorId = "8500";
		
		JsonNode updatedObject = this.getJsonObject(newFornecedorId);
		
		repo.update(INDEX, TYPE, ID, updatedObject);
		
		JsonNode response = repo.getById(INDEX, TYPE, ID);
		
		assertThat(response.get("fornecedor_id").toString(), 
				is(updatedObject.get("fornecedor_id").toString()));
		
	}
	
	@Test
	public void testDeleteById() {
		DeleteResponse response = this.repo.deleteById(INDEX, TYPE, ID);
		
		assertThat(response.getResult().toString(), is("NOT_FOUND"));
	}
	
	@Test
	public void testDeleteIndex() {
		DeleteIndexResponse response = this.repo.deleteIndex(INDEX);
				
		assertTrue(response.isAcknowledged());
	}
		
	@After
	public void tearDown() {
		this.repo.deleteById(INDEX, TYPE, ID);
	}
	
	private JsonNode getJsonObject(String fornecedorId) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		
		String jsonString = "{\"fornecedor_id\": \"" + fornecedorId + "\",\n" +
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
