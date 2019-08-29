package br.com.github.lassulfi.respository.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import br.com.github.lassulfi.repository.ElasticsearchRepository;
import br.com.github.lassulfi.repository.exception.ElasticsearchException;
import br.com.github.lassulfi.repository.impl.ElasticsearchRepositoryImpl;

public class ElasticsearchRepositoryImplTest {

	private ElasticsearchRepository repo;

	private JsonNode jsonObject;

	private static final String ID = UUID.randomUUID().toString();
	private static final String INDEX = "likes";
	private static final String TYPE = "_doc";

	@Before
	public void setup() throws Exception {
		repo = new ElasticsearchRepositoryImpl();

		jsonObject = this.getJsonObject();
	}

	@Test
	public void testCreateIndex() throws ElasticsearchException {
		String newIndex = "tests";

		this.repo.deleteIndex(newIndex);

		CreateIndexResponse response = this.repo.createIndex(newIndex, null);

		assertThat(response.index(), is(newIndex));

	}

	@Test
	public void testInsertObject() throws ElasticsearchException {
		IndexResponse response = repo.insert(INDEX, TYPE, ID, jsonObject);

		assertThat(response.getId(), is(ID));
	}

	@Test
	public void testFindById() throws ElasticsearchException {
		repo.insert(INDEX, TYPE, ID, jsonObject);

		JsonNode jsonObject = repo.getById(INDEX, TYPE, ID);

		assertNotNull(jsonObject);
	}

	@Test
	public void testGetByScript() throws ElasticsearchException {
		repo.insert(INDEX, TYPE, ID, jsonObject);

		String script = "int total = 0;" + "for (int i = 0; i < doc['likes'].length; ++i) {" + "++total;" + "}"
				+ "return total;";

		JsonNode result = this.repo.getByScript(INDEX, script, null);

		assertNotNull(result);
	}

	@Test
	public void testGetByMapping() throws Exception {
		this.repo.deleteIndex(INDEX);

		String[] jsonArray = {
				"{\n" + "	\"fornecedor_id\":\"65bbdd07-3be6-48f0-99b2-f6172272e839\",\n"
						+ "	\"user_id\":\"7ea9aa7f-e475-4942-80c0-80502387b276\",\n" + "	\"like\":1,\n"
						+ "	\"dislike\":0\n" + "}",
				"{\n" + "	\"fornecedor_id\":\"65bbdd07-3be6-48f0-99b2-f6172272e839\",\n"
						+ "	\"user_id\":\"7ea9aa7f-e475-4942-80c0-80502387b277\",\n" + "	\"like\":1,\n"
						+ "	\"dislike\":0\n" + "}",
				"{\n" + "	\"fornecedor_id\":\"65bbdd07-3be6-48f0-99b2-f6172272e839\",\n"
						+ "	\"user_id\":\"7ea9aa7f-e475-4942-80c0-80502387b278\",\n" + "	\"like\":0,\n"
						+ "	\"dislike\":1\n" + "}" };

		ObjectMapper mapper = this.getMapper();

		for (String str : jsonArray) {
			JsonNode jsonObject = mapper.readTree(str);
			this.repo.insert(INDEX, TYPE, UUID.randomUUID().toString(), jsonObject);
		}
		
		Map<String, String> query = new HashMap<String, String>();
		query.put("fornecedor_id", "65bbdd07-3be6-48f0-99b2-f6172272e839");
		query.put("like", "1");
		
		SearchResponse response = this.repo.getByMapping(INDEX, query);
		
		assertNotNull(response);
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
	public void testUpsert() throws Exception {
		this.repo.deleteIndex(INDEX);

		UpdateResponse response = this.repo.update(INDEX, TYPE, ID, this.getJsonObject());

		assertThat(response.getResult().toString(), is("CREATED"));
	}

	@Test
	public void testUpdateMapping() throws Exception {
		repo.insert(INDEX, TYPE, ID, jsonObject);

		String jsonUpdateString = "{\n" + "	\"like\":\n" + "			\"user_10\"\n" + "}";

		ObjectMapper mapper = new ObjectMapper();
		JsonNode updateObject = mapper.readTree(jsonUpdateString);

		String script = "ctx._source.likes.add(params.like)";

		repo.updateMapping(INDEX, TYPE, ID, script, updateObject);

		JsonNode response = repo.getById(INDEX, TYPE, ID);

		assertThat(response.get("likes").toString(), is("[\"user_01\",\"user_02\",\"user_10\"]"));

	}

	@Test
	public void testDeleteById() throws Exception {
		this.repo.insert(INDEX, TYPE, ID, this.getJsonObject());

		DeleteResponse response = this.repo.deleteById(INDEX, TYPE, ID);

		assertThat(response.getResult().toString(), is("DELETED"));
	}

	@Test
	public void testDeleteIndex() throws ElasticsearchException {

		DeleteIndexResponse response = this.repo.deleteIndex(INDEX);

		assertTrue(response.isAcknowledged());
	}

	@Test
	public void testCount() throws IOException, ElasticsearchException {
		this.repo.deleteIndex(INDEX);

		String[] jsonArray = {
				"{\n" + "	\"fornecedor_id\":\"65bbdd07-3be6-48f0-99b2-f6172272e839\",\n"
						+ "	\"user_id\":\"7ea9aa7f-e475-4942-80c0-80502387b276\",\n" + "	\"like\":1,\n"
						+ "	\"dislike\":0\n" + "}",
				"{\n" + "	\"fornecedor_id\":\"65bbdd07-3be6-48f0-99b2-f6172272e839\",\n"
						+ "	\"user_id\":\"7ea9aa7f-e475-4942-80c0-80502387b277\",\n" + "	\"like\":1,\n"
						+ "	\"dislike\":0\n" + "}",
				"{\n" + "	\"fornecedor_id\":\"65bbdd07-3be6-48f0-99b2-f6172272e839\",\n"
						+ "	\"user_id\":\"7ea9aa7f-e475-4942-80c0-80502387b278\",\n" + "	\"like\":0,\n"
						+ "	\"dislike\":1\n" + "}" };

		ObjectMapper mapper = this.getMapper();

		for (String str : jsonArray) {
			JsonNode jsonObject = mapper.readTree(str);
			this.repo.insert(INDEX, TYPE, UUID.randomUUID().toString(), jsonObject);
		}

		Map<String, String> queryTerms = new HashMap<String, String>();
		queryTerms.put("fornecedor_id", "65bbdd07-3be6-48f0-99b2-f6172272e839");

		Map<String, String> countTerms = new HashMap<String, String>();
		countTerms.put("fornecedor", "fornecedor_id.keyword");

		Map<String, String> countFields = new HashMap<String, String>();
		countFields.put("total_likes", "like");

		SearchResponse response = this.repo.count(new String[] { INDEX }, queryTerms, countTerms, countFields);

		assertNotNull(response);
	}

	@After
	public void tearDown() throws ElasticsearchException {
		this.repo.deleteIndex(INDEX);
	}

	private JsonNode getJsonObject() throws Exception {
		ObjectMapper mapper = new ObjectMapper();

		String jsonString = "{\n" + "	\"fornecedor_id\":\"" + ID + "\",\n"
				+ "	\"user_id\":\"7ea9aa7f-e475-4942-80c0-80502387b276\",\n" + "	\"like\":1,\n" + "	\"dislike\":0\n"
				+ "}";

		return mapper.readTree(jsonString);
	}

	private ObjectMapper getMapper() {
		return new ObjectMapper();
	}
}
