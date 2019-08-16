package br.com.github.lassulfi.repository;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;

import com.fasterxml.jackson.databind.JsonNode;

public interface ElasticsearchRepository {
	
	IndexResponse insert(String index, String id, JsonNode jsonObject);
	
	JsonNode getById(String index, String id);
	
	UpdateResponse update(String index, String type, String id, JsonNode jsonObject);
	
	void deleteById(String index, String type, String id);	
	
}
