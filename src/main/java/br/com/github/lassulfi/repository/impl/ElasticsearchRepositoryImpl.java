package br.com.github.lassulfi.repository.impl;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import br.com.github.lassulfi.repository.ElasticsearchRepository;

public class ElasticsearchRepositoryImpl implements ElasticsearchRepository {

	private static final String HOST = "localhost";
	private static final int PORT_ONE = 9200;
	private static final int PORT_TWO = 9300;
	private static final String SCHEME = "http";

	public IndexResponse insert(String index, String id, JsonNode jsonObject) {
		RestHighLevelClient client = this.getClient();
		
		IndexRequest request = new IndexRequest();
		request.index(index)
			.id(id)
			.source(jsonObject, XContentType.JSON);	
		
		IndexResponse response = null;		
		try {
			response = client.index(request, RequestOptions.DEFAULT);
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

	public JsonNode getById(String index, String id) {
		RestHighLevelClient client = this.getClient();
		
		GetRequest request = new GetRequest(index, id);
		
		GetResponse response = null;
		try {
			response = client.get(request, RequestOptions.DEFAULT);
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
		// TODO Auto-generated method stub
		return null;
	}

	public void deleteById(String index, String type, String id) {
		// TODO Auto-generated method stub

	}

	private RestHighLevelClient getClient() {
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost(HOST, PORT_ONE, SCHEME), 
						new HttpHost(HOST, PORT_TWO, SCHEME)));
		
		return client;
	}

}
