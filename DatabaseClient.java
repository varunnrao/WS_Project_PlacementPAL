package com.vnr;

import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.ws.rs.core.MediaType;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.representation.Form;


public class DatabaseClient {
	private Client client;
	private String baseURL;
	private String tokenID;
	public DatabaseClient(String url, String username, String password) {
		this.client = Client.create();	
		this.baseURL = url;
		Form loginForm = new Form();
		loginForm.add("user_name", username); 
		loginForm.add("password", password);
		loginForm.add("inputFormat", "FORM_URLENCODED");
		loginForm.add("outputFormat", "json");
		String response = this.client.resource(this.baseURL + "/login/")
									 .type(MediaType.APPLICATION_FORM_URLENCODED)
									 .accept(MediaType.APPLICATION_JSON)
									 .post(String.class, loginForm);
		JsonObject jsonResponse = Json.createReader(new StringReader(response)).readObject();
		/* TODO:
		 * Handle responses of status codes 401, 404, and 500. 
		 * Optimism lives loudly within me. 
		 */		
		this.tokenID = jsonResponse.getString("token");
	}
	
	public JsonObject insert(String tableName, List<Map<String, Object>> rows) {		
		JsonBuilderFactory jsonBuilderFactory = Json.createBuilderFactory(null);
		JsonArrayBuilder jsonArrayBuilder = jsonBuilderFactory.createArrayBuilder();
		for(Map<String, Object> row: rows) {
			jsonArrayBuilder.add(jsonBuilderFactory.createObjectBuilder(row));
		}
		Form insertForm = new Form();
		insertForm.add("token", this.tokenID);		
		insertForm.add("data", jsonArrayBuilder.build());
		String response = this.client.resource(this.baseURL + "/table/insert/" + tableName)
									 .type(MediaType.APPLICATION_FORM_URLENCODED)
									 .accept(MediaType.APPLICATION_JSON)
									 .post(String.class, insertForm);
		return Json.createReader(new StringReader(response)).readObject();
	}
	
	public JsonObject select(String tableName, List<String> columns, Optional<String> whereClause) {
		String queryString = "token="      + this.tokenID + "&" +
							 "table_name=" + tableName + "&" + 							 
				             "columns="    + String.join(",", columns);
		
		if(whereClause.isPresent()) {
			queryString += ("&where=" + whereClause.get());
		}
		String response = this.client.resource(this.baseURL + "/select?" + queryString)
				                     .accept(MediaType.APPLICATION_JSON)
				                     .get(String.class);
		return Json.createReader(new StringReader(response)).readObject();
		
	}
	
	public JsonObject update(String tableName, Map<String, Object> setClause, Optional<Map<String, Object>> whereClause) {
		String setClauseString = setClause.entrySet().stream()
                .map(keyValuePair -> keyValuePair.getKey() + "=" + keyValuePair.getValue().toString())
                .reduce((first, second) -> first + " AND " + second)
                .orElse("");  
		Form updateForm = new Form();
		updateForm.add("token", this.tokenID);
		updateForm.add("set", setClauseString);
		if(whereClause.isPresent()) {
			String whereClauseString = whereClause.get().entrySet().stream()
					                                    .map(keyValuePair -> keyValuePair.getKey() + "=" + keyValuePair.getValue().toString())
					                                    .reduce((first, second) -> first + " AND " + second)
					                                    .orElse("");                                
			updateForm.add("where", whereClauseString);	   
		}
		String response = this.client.resource(this.baseURL + "/delete/" + tableName)
				.type(MediaType.APPLICATION_FORM_URLENCODED)
                .accept(MediaType.APPLICATION_JSON)
                .put(String.class, updateForm);
		return Json.createReader(new StringReader(response)).readObject();
	} 
	
	public JsonObject delete(String tableName, Optional<Map<String, Object>> whereClause) {
		Form deleteForm = new Form();
		deleteForm.add("token",	this.tokenID);
		if(whereClause.isPresent()) {
			String whereClauseString = whereClause.get().entrySet().stream()
					                                    .map(keyValuePair -> keyValuePair.getKey() + "=" + keyValuePair.getValue().toString())
					                                    .reduce((first, second) -> first + " AND " + second)
					                                    .orElse("");                                
			deleteForm.add("where", whereClauseString);	   
		}
		String response = this.client.resource(this.baseURL + "/delete/" + tableName)
									 .type(MediaType.APPLICATION_FORM_URLENCODED)
				                     .accept(MediaType.APPLICATION_JSON)
				                     .delete(String.class, deleteForm);
		return Json.createReader(new StringReader(response)).readObject();		
	} 
	
	public JsonObject dropTable(String tableName) {
		Form dropTableForm = new Form();
		dropTableForm.add("token", this.tokenID);
		String response = this.client.resource(this.baseURL + "/drop/" + tableName)
									 .type(MediaType.APPLICATION_FORM_URLENCODED)
				                     .accept(MediaType.APPLICATION_JSON)
				                     .delete(String.class, dropTableForm);
		return Json.createReader(new StringReader(response)).readObject();
	}

}