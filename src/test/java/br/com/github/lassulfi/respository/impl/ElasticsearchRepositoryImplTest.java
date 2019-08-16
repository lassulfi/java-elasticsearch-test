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
	private static final String INDEX = "menus";
	private static final String TYPE = "menu";
	
	@Before
	public void setup() throws Exception {
		repo = new ElasticsearchRepositoryImpl();
		
		jsonObject = this.getJsonObject();
	}
	
	@Test
	public void testInsertObject() {
		IndexResponse response = repo.insert(INDEX, ID, jsonObject);
		
		assertThat(response.getId(), is(ID));		
	}
		
	@After
	public void tearDown() {
		this.repo.deleteById(INDEX, TYPE, ID);
	}
	
	private JsonNode getJsonObject() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		
		String jsonString = "{\"menu\": {\n" + 
				"  \"id\": \"file\",\n" + 
				"  \"value\": \"File\",\n" + 
				"  \"popup\": {\n" + 
				"    \"menuitem\": [\n" + 
				"      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" + 
				"      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" + 
				"      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n" + 
				"    ]\n" + 
				"  }\n" + 
				"}}";
		
		return mapper.readTree(jsonString);
	}
}
