package br.com.github.lassulfi.repository;

import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;

import com.fasterxml.jackson.databind.JsonNode;

import br.com.github.lassulfi.repository.exception.ElasticsearchException;

public interface ElasticsearchRepository {
	
	CreateIndexResponse createIndex(String index, JsonNode mappingSource) throws ElasticsearchException;
	
	IndexResponse insert(String index, String type, String id, JsonNode jsonObject) throws ElasticsearchException;
	
	JsonNode getById(String index, String type, String id) throws ElasticsearchException;
	
	JsonNode getByScript(String index, String query, JsonNode mappingParams) throws ElasticsearchException;
	
	SearchResponse getByMapping(String index, Map<String, String> query) throws ElasticsearchException;
	
	UpdateResponse updateMapping(String index, String type, String id, String script, JsonNode mappingParams)
			throws ElasticsearchException;
	
	UpdateResponse update(String index, String type, String id, JsonNode jsonObject) throws ElasticsearchException;
	
	DeleteResponse deleteById(String index, String type, String id) throws ElasticsearchException;
	
	DeleteIndexResponse deleteIndex(String index) throws ElasticsearchException;
	
	SearchResponse count(String[] indices, Map<String, String> queryTerms, Map<String, String> countTerms, 
			Map<String, String> countFields) throws ElasticsearchException;
	
}
