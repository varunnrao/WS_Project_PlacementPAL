package com.vnr;

import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;



public class DatabaseClient {
	private Client client;
	private String baseURL;
	private String tokenID;
	public DatabaseClient(String url, String username, String password) {
		this.client = ClientBuilder.newClient();	
		this.baseURL = url;
		Form loginForm = new Form();
		loginForm.param("user_name", username); 
		loginForm.param("password", password);
		String response = this.client.target(this.baseURL + "/login/")
				                     .request(MediaType.APPLICATION_JSON)
				                     .post(Entity.entity(loginForm, MediaType.APPLICATION_FORM_URLENCODED_TYPE), String.class);
		JsonObject jsonResponse = Json.createReader(new StringReader(response)).readObject();
		this.tokenID = jsonResponse.getString("token");
	}
	
	public Future<Response> insert(String tableName, List<Map<String, Object>> rows) {		
		JsonBuilderFactory jsonBuilderFactory = Json.createBuilderFactory(null);
		JsonArrayBuilder jsonArrayBuilder = jsonBuilderFactory.createArrayBuilder();
		for(Map<String, Object> row: rows) {
			jsonArrayBuilder.add(jsonBuilderFactory.createObjectBuilder(row));
		}
		Form insertForm = new Form();
		insertForm.param("token", this.tokenID);		
		insertForm.param("data", jsonArrayBuilder.build().toString());
		return this.client.target(this.baseURL + "/table/insert/" + tableName)
			              .request(MediaType.APPLICATION_JSON)
			              .async().post(Entity.entity(insertForm, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
	}
	
	public Future<Response> select(List<String> tables, List<String> columns, Optional<String> whereClause, Optional<Integer> limit) {
		WebTarget target = client.target(this.baseURL + "/select?")
								 .queryParam("token", this.tokenID)
								 .queryParam("table_name", String.join(",", tables))
								 .queryParam("columns", String.join(",", columns));
		if(whereClause.isPresent()) {
			target.queryParam("where", whereClause.get());
		}	
		if(limit.isPresent())
			target.queryParam("limit",limit.get());
		return target.request(MediaType.APPLICATION_JSON)
					 .async().get();		
	}
	
	public Future<Response> update(String tableName, Map<String, Object> setClause, Optional<Map<String, Object>> whereClause) {
		String setClauseString = setClause.entrySet().stream()
                .map(keyValuePair -> keyValuePair.getKey() + "=" + keyValuePair.getValue().toString())
                .reduce((first, second) -> first + " AND " + second)
                .orElse("");  
		Form updateForm = new Form();
		updateForm.param("token", this.tokenID);
		updateForm.param("set", setClauseString);
		if(whereClause.isPresent()) {
			String whereClauseString = whereClause.get().entrySet().stream()
					                                    .map(keyValuePair -> keyValuePair.getKey() + "=" + keyValuePair.getValue().toString())
					                                    .reduce((first, second) -> first + " AND " + second)
					                                    .orElse("");                                
			updateForm.param("where", whereClauseString);	   
		}		
		return this.client.target(this.baseURL + "/delete/" + tableName)
				          .request(MediaType.APPLICATION_JSON)
				          .async().put(Entity.entity(updateForm, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
	} 
	
	public Future<Response> delete(String tableName, Optional<Map<String, Object>> whereClause) {
		Form deleteForm = new Form();
		deleteForm.param("token", this.tokenID);
		if(whereClause.isPresent()) {
			String whereClauseString = whereClause.get().entrySet().stream()
					                                    .map(keyValuePair -> keyValuePair.getKey() + "=" + keyValuePair.getValue().toString())
					                                    .reduce((first, second) -> first + " AND " + second)
					                                    .orElse("");                                
			deleteForm.param("where", whereClauseString);	   
		}
		return this.client.target(this.baseURL + "/delete/" + tableName)
				          .request(MediaType.APPLICATION_JSON)
				          .async().method("DELETE", Entity.entity(deleteForm, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
	} 
	
	public Future<Response> dropTable(String tableName) {
		Form dropTableForm = new Form();
		dropTableForm.param("token", this.tokenID);
		return this.client.target(this.baseURL + "/drop/" + tableName)
		           .request(MediaType.APPLICATION_JSON)
		           .async().method("DELETE", Entity.entity(dropTableForm, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
	}

}
