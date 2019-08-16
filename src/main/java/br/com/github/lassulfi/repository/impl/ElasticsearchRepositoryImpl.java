package br.com.github.lassulfi.repository.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

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
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (IOException e) {
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
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		ObjectMapper mapper = new ObjectMapper();
		
		return response != null ? mapper.convertValue(response.getSourceAsMap(), JsonNode.class) : null;
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
		request.upsert(jsonString, XContentType.JSON);
		
		UpdateResponse response = null;
		
		try {
			response = client.update(request);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return response;
	}

	public void deleteById(String index, String type, String id) {
		// TODO Auto-generated method stub

	}
	
	private ObjectMapper getMapper() {
		ObjectMapper mapper = new ObjectMapper();
		
		return mapper;
	}

	private RestHighLevelClient getClient() {
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost(HOST, PORT_ONE, SCHEME), 
						new HttpHost(HOST, PORT_TWO, SCHEME)));
		
		return client;
	}

}
