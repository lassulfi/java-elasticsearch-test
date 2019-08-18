package br.com.github.lassulfi.repository;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;

import com.fasterxml.jackson.databind.JsonNode;

public interface ElasticsearchRepository {
	
	IndexResponse insert(String index, String type, String id, JsonNode jsonObject);
	
	JsonNode getById(String index, String type, String id);
	
	UpdateResponse update(String index, String type, String id, JsonNode jsonObject);
	
	DeleteResponse deleteById(String index, String type, String id);
	
	DeleteIndexResponse deleteIndex(String index);
	
}
