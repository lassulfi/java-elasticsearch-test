package br.com.github.lassulfi.repository.impl;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import br.com.github.lassulfi.repository.ElasticsearchRepository;

public class ElasticsearchRepositoryImpl implements ElasticsearchRepository {

	private static final String HOST = "localhost";
	private static final int PORT_ONE = 9200;
	private static final int PORT_TWO = 9300;
	private static final String SCHEME = "http";

	public IndexResponse insert(String index, String type, String id, JsonNode jsonObject) {
		RestHighLevelClient client = this.getClient();

		IndexRequest request = new IndexRequest(index, type, id);

		ObjectMapper mapper = this.getMapper();

		String jsonString = null;

		try {
			jsonString = mapper.writeValueAsString(jsonObject);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		request.source(jsonString, XContentType.JSON);

		IndexResponse response = null;
		try {
			response = client.index(request);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return response;
	}

	public JsonNode getById(String index, String type, String id) {
		RestHighLevelClient client = this.getClient();
			
		GetRequest request = new GetRequest(index, type, id);

		GetResponse response = null;
		try {
			response = client.get(request);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		ObjectMapper mapper = new ObjectMapper();

		return response != null ? mapper.convertValue(response.getSourceAsMap(), JsonNode.class) : null;
	}
	
	public JsonNode getByScript(String index, String scriptQuery, JsonNode mappingParams) {
		RestHighLevelClient client = this.getClient();
		
		SearchRequest request = new SearchRequest(index);	
		request
			.source()
			.query(QueryBuilders.scriptQuery(this.generateScript(scriptQuery, mappingParams)));
		
		SearchResponse response = null;
		
		try {
			response = client.search(request);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		ObjectMapper mapper = this.getMapper();
		
		return response != null ? mapper.convertValue(response, JsonNode.class) : null;
	}

	public UpdateResponse update(String index, String type, String id, JsonNode jsonObject) {
		RestHighLevelClient client = this.getClient();

		UpdateRequest request = new UpdateRequest(index, type, id);

		ObjectMapper mapper = this.getMapper();

		String jsonString = null;
		try {
			jsonString = mapper.writeValueAsString(jsonObject);
		} catch (Exception e) {
			e.printStackTrace();
		}

		request.doc(jsonString, XContentType.JSON);
		request.upsert(XContentType.JSON);

		UpdateResponse response = null;

		try {
			response = client.update(request);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return response;
	}

	public UpdateResponse updateMapping(String index, String type, String id, String script, JsonNode mappingParams) {
		RestHighLevelClient client = this.getClient();

		UpdateRequest request = new UpdateRequest(index, type, id);

		request.script(this.generateScript(script, mappingParams));
		request.upsert(XContentType.JSON);

		UpdateResponse response = null;

		try {
			response = client.update(request);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return response;
	}

	public DeleteResponse deleteById(String index, String type, String id) {
		RestHighLevelClient client = this.getClient();

		DeleteRequest request = new DeleteRequest(index, type, id);

		DeleteResponse response = null;

		try {
			response = client.delete(request);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return response;
	}

	public DeleteIndexResponse deleteIndex(String index) {
		RestHighLevelClient client = this.getClient();

		DeleteIndexRequest request = new DeleteIndexRequest(index);
		request.indicesOptions(IndicesOptions.lenientExpandOpen());

		DeleteIndexResponse response = null;

		try {
			response = client.indices().delete(request);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return response;

	}

	private ObjectMapper getMapper() {
		ObjectMapper mapper = new ObjectMapper();

		return mapper;
	}

	private RestHighLevelClient getClient() {
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost(HOST, PORT_ONE, SCHEME), new HttpHost(HOST, PORT_TWO, SCHEME)));

		return client;
	}

	private Script generateScript(String script, JsonNode mappingParams) {
		ObjectMapper mapper = this.getMapper();
		Map<String, Object> mapObject = mapper.convertValue(mappingParams, Map.class);

		if(mappingParams != null) {
			return new Script(ScriptType.INLINE, "painless", script, mapObject);
		} else {
			return new Script(script);
		}
	}
	
	private boolean findIndex(String index) {
		RestHighLevelClient client = this.getClient();
		
		IndexRequest indexRequest = new IndexRequest(index);
		
		IndexResponse response = null;
		try {
			response = client.index(indexRequest);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return response == null ? false : true;
	}

}
