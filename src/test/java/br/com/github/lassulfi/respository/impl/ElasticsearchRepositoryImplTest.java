package br.com.github.lassulfi.respository.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
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
	private static final String TYPE = "json";
	
	private static final String FORNECEDOR_ID = UUID.randomUUID().toString();
	
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
	
	@Test
	public void testFindById() {
		repo.insert(INDEX, TYPE, ID, jsonObject);
		
		JsonNode jsonObject = repo.getById(INDEX, TYPE, ID);
		
		assertThat(jsonObject.get("fornecedor_id").asText(), is(FORNECEDOR_ID));
	}
	
	@Test
	public void testGetByScript() {
				
		String script = "int total = 0;"
				+ "for (int i = 0; i < doc['likes'].length; ++i) {"
				+ "++total;"
				+ "}"
				+ "return total;";
		
		JsonNode result = this.repo.getByScript(INDEX, script, null);
		
		assertNotNull(result);
	}
	
	@Test
	public void testUpdate() throws Exception {
		repo.insert(INDEX, TYPE, ID, jsonObject);		
		
		String updateJson = "{\"likes\":\"user_10\"}";

		ObjectMapper mapper = new ObjectMapper();		
		JsonNode updateObject = mapper.readTree(updateJson);
		
		UpdateResponse response = repo.update(INDEX, TYPE, ID, updateObject);
	
		JsonNode result = this.repo.getById(INDEX, TYPE, ID);
		
		assertThat(result.get("likes").toString(), is("\"user_10\""));
	}
	
	@Test
	public void testUpdateMapping() throws Exception {
		repo.insert(INDEX, TYPE, ID, jsonObject);
		
		String jsonUpdateString = "{\n" + 
				"	\"like\":\n" + 
				"			\"user_10\"\n"
				+ "}";
		
		ObjectMapper mapper = new ObjectMapper();
		JsonNode updateObject = mapper.readTree(jsonUpdateString);
		
		String script = "ctx._source.likes.add(params.like)";
		
		repo.updateMapping(INDEX, TYPE, ID, script, updateObject);
		
		JsonNode response = repo.getById(INDEX, TYPE, ID);
		
		//TODO: correct the assertion
		assertThat(response.get("likes").toString(), 
				is("[\"user_01\",\"user_02\",\"user_10\"]"));
		
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
	
	private JsonNode getJsonObject() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		
		String jsonString = "{\n" + 
				"	\"likes\":[\n" + 
				"			\"user_01\",\n" + 
				"			\"user_02\"\n" + 
				"		],\n" + 
				"	\"dislikes\":[\n" + 
				"		\"user_03\"\n" + 
				"		]\n" + 
				"}";
		
		return mapper.readTree(jsonString);
	}
}
