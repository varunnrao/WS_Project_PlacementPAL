package com.vnr;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.servlet.ServletContext;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import jdk.nashorn.api.scripting.JSObject;

@Path("/")
public class PlacementPal {
	
	String url_gd = "http://api.glassdoor.com/api/api.htm?t.p=208265&t.k=cjGeqw2LUI&userip=0.0.0.0&useragent=&format=json&v=1&action=employers&q=";
	String url_fc = "https://api.fullcontact.com/v2/company/lookup.json?domain=";
	final String fc_api_key = "ff7d5aaa42c5cee1";
	Client client = ClientBuilder.newClient();
	DatabaseClient database = new DatabaseClient("<database-url>", "<username>", "password");
	
	@Context
	private ServletContext sctx;	

	@GET
	@Path("/get/all/{companyName: [a-zA-Z]+}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getAllJSON(@PathParam("companyName") String companyName) {		
		JsonObject gd = get_gd(companyName);
		JsonObject fc = get_fc(companyName);
		JsonObject finalStruct = Json.createObjectBuilder()
				.add("organization", fc.getJsonObject("organization"))
				.add("website", fc.getJsonString("website"))
				.add("socialProfiles", fc.getJsonArray("socialProfiles"))
				.add("ratings", gd.getJsonObject("ratings"))
				.add("featuredReview", gd.getJsonObject("featuredReview"))
				.add("ceo",gd.getJsonObject("ceo"))
			.build();
		return Response.ok(finalStruct.toString(),MediaType.APPLICATION_JSON).build();
	}	
	
	@GET
	@Path("/get/ratings/{companyName: [a-zA-Z]+}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getRatings(@PathParam("companyName") String companyName) {
		JsonObject fs = get_gd(companyName);	
		if(!fs.containsKey("status")) {
			return Response.ok(fs.getJsonObject("ratings").toString(),MediaType.APPLICATION_JSON).build();
		}
		else {
			JsonObject err = Json.createObjectBuilder().add("code","404").add("msg", companyName + " is unavailable for querying").build();
			JsonObject ceo = Json.createObjectBuilder().add("status", err.getJsonString("code")).add("msg", err.getJsonString("msg")).build();
			return Response.ok(ceo.toString(),MediaType.APPLICATION_JSON).build();						
			
		}		
	}
	
	@GET
	@Path("/get/contact/{companyName: [a-zA-Z]+}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getContact(@PathParam("companyName") String companyName) {
		JsonObject fs = get_fc(companyName)	;
		JsonObject contactInfo;		
		if(!fs.containsKey("status")) {
			JsonObject addrAndPhNo = fs.getJsonObject("organization").getJsonObject("contactInfo");
			JsonString website = fs.getJsonString("website");
			JsonObject succ = Json.createObjectBuilder().add("code", "200").build();
			contactInfo = Json.createObjectBuilder()
					.add("status", succ.getJsonString("code"))
					.add("Contact Details", addrAndPhNo)
					.add("Website", website)
				.build();			
		}
		else {
			JsonObject err = Json.createObjectBuilder().add("code","404").add("msg", companyName + " is unavailable for querying").build();
			contactInfo = Json.createObjectBuilder().add("status", err.getJsonString("code")).add("msg", err.getJsonString("msg")).build();
		}		
		return Response.ok(contactInfo.toString(),MediaType.APPLICATION_JSON).build();		
	}
	
	@GET
	@Path("/get/overview/{companyName: [a-zA-Z]+}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getOverview(@PathParam("companyName") String companyName) {
		JsonObject fs = get_fc(companyName);
		JsonObject overview;
		if(!fs.containsKey("status")) {
			JsonNumber approxEmps = fs.getJsonObject("organization").getJsonNumber("approxEmployees");
			JsonString overviewText = fs.getJsonObject("organization").getJsonString("overview");
			JsonArray kw = fs.getJsonObject("organization").getJsonArray("keywords");
			JsonObject succ = Json.createObjectBuilder().add("code", "200").build();
			
			overview = Json.createObjectBuilder()
					.add("status", succ.getJsonString("code"))
					.add("approxEmployees", approxEmps)
					.add("overview", overviewText)
					.add("keywords", kw)
				.build();		
		}		
		else {
			JsonObject err = Json.createObjectBuilder().add("code","404").add("msg", companyName + " is unavailable for querying").build();
			overview = Json.createObjectBuilder().add("status", err.getJsonString("code")).add("msg", err.getJsonString("msg")).build();			
		}
		return Response.ok(overview.toString(),MediaType.APPLICATION_JSON).build();		
	}
	
	@GET
	@Path("/get/ceo/{companyName: [a-zA-Z]+}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getCEO(@PathParam("companyName") String companyName) 	{
		JsonObject fs = get_gd(companyName);		
		if(!fs.containsKey("status")) {
			return Response.ok(fs.getJsonObject("ceo").toString(),MediaType.APPLICATION_JSON).build();			
		}
		else {
			JsonObject err = Json.createObjectBuilder().add("code","404").add("msg", companyName + " is unavailable for querying").build();
			JsonObject ceo = Json.createObjectBuilder().add("status", err.getJsonString("code")).add("msg", err.getJsonString("msg")).build();
			return Response.ok(ceo.toString(),MediaType.APPLICATION_JSON).build();		
		}		
	}
	
	@GET
	@Path("/get/social/{companyName: [a-zA-Z]+}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getSocial(@PathParam("companyName") String companyName) {
		JsonObject fs = get_fc(companyName);		
		if(!fs.containsKey("status")) {
			return Response.ok(fs.getJsonArray("socialProfiles").toString(),MediaType.APPLICATION_JSON).build();			
		}		
		else {
			JsonObject err = Json.createObjectBuilder().add("code","404").add("msg", companyName + " is unavailable for querying").build();
			JsonObject social = Json.createObjectBuilder().add("status", err.getJsonString("code")).add("msg", err.getJsonString("msg")).build();
			return Response.ok(social.toString(),MediaType.APPLICATION_JSON).build();						
		}/*		
		JsonArray arr = fs.getJsonArray("socialProfiles");
		JsonObjectBuilder b = Json.createObjectBuilder();
		for(int i = 0; i<arr.size(); ++i)
		{
			JsonObject t = arr.getJsonObject(i);
			// somehow keep adding to an object
			// "typeName" and "url" fields
		}*/
	}

	@GET
	@Path("/get/review/featured/{country}/{companyName: [a-zA-Z]+}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getReviewsCountry(@PathParam("country") String country,@PathParam("companyName") String companyName) {
		JsonObject fs = get_gd(companyName,country);	
		if(!fs.containsKey("status")) {
			return Response.ok(fs.getJsonObject("featuredReview").toString(),MediaType.APPLICATION_JSON).build();
		}
		else {
			JsonObject err = Json.createObjectBuilder().add("code","404").add("msg", companyName + " is unavailable for querying in " + country).build();
			JsonObject review = Json.createObjectBuilder().add("status", err.getJsonString("code")).add("msg", err.getJsonString("msg")).build();
			return Response.ok(review.toString(),MediaType.APPLICATION_JSON).build();				
		}
	}
	
	@GET
	@Path("/get/review/featured/{companyName: [a-zA-Z]+}/")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getReviews(@PathParam("companyName") String companyName) {
		JsonObject fs = get_gd(companyName);		
		if(!fs.containsKey("status")) {
			return Response.ok(fs.getJsonObject("featuredReview").toString(),MediaType.APPLICATION_JSON).build();			
		}
		else {
			JsonObject err = Json.createObjectBuilder().add("code","404").add("msg", companyName + " is unavailable for querying").build();
			JsonObject review = Json.createObjectBuilder().add("status", err.getJsonString("code")).add("msg", err.getJsonString("msg")).build();
			return Response.ok(review.toString(),MediaType.APPLICATION_JSON).build();			
		}
	}

	
	private JsonObject gd_driver(String url) {
        String gd_response = client.target(url).request(MediaType.APPLICATION_JSON).get(String.class);
        System.out.print(gd_response);
		JsonReader jsonReader_gd = Json.createReader(new StringReader(gd_response));
		JsonObject jobj_gd = jsonReader_gd.readObject();
		JsonObject finalStruct;
		int numOfResults = jobj_gd.getJsonObject("response").getJsonNumber("totalRecordCount").intValueExact();
	
		if(numOfResults != 0) {
			JsonObject emp = (JsonObject) jobj_gd.getJsonObject("response").getJsonArray("employers").get(0);
			JsonString r1 = emp.getJsonString("overallRating");
			JsonString r2 = emp.getJsonString("ratingDescription");
			JsonString r3 = emp.getJsonString("cultureAndValuesRating");
			JsonString r4 = emp.getJsonString("seniorLeadershipRating");
			JsonString r5 = emp.getJsonString("compensationAndBenefitsRating");
			JsonString r6 = emp.getJsonString("careerOpportunitiesRating");
			JsonString r7 = emp.getJsonString("workLifeBalanceRating");
			JsonNumber r8 = emp.getJsonNumber("recommendToFriendRating");
			JsonObject featRev = emp.getJsonObject("featuredReview");
			JsonString loc = featRev.getJsonString("location");
			JsonString pros = featRev.getJsonString("pros");
			JsonString cons = featRev.getJsonString("cons");
			JsonString headline = featRev.getJsonString("headline");
			JsonObject ceo = emp.getJsonObject("ceo");
			JsonString ceoName = ceo.getJsonString("name");
			JsonNumber numberOfRatings = ceo.getJsonNumber("numberOfRatings");
			JsonNumber pctApprove = ceo.getJsonNumber("pctApprove");
			JsonNumber pctDisapprove = ceo.getJsonNumber("pctDisapprove");			
			finalStruct = Json.createObjectBuilder()
							.add("ratings", Json.createObjectBuilder()								
								.add("overallRating", r1)
								.add("ratingDescription", r2)
								.add("cultureAndValuesRating", r3)
								.add("seniorLeadershipRating", r4)
								.add("compensationAndBenefitsRating", r5)
								.add("careerOpportunitiesRating", r6)
								.add("workLifeBalanceRating", r7)
								.add("recommendToFriendRating", r8))							
							.add("featuredReview", Json.createObjectBuilder()
								.add("location", loc)
								.add("pros", pros)
								.add("cons", cons)
								.add("headline", headline))
							.add("ceo", Json.createObjectBuilder()
								.add("name", ceoName)
								.add("numberOfRatings", numberOfRatings)
								.add("pctApprove", pctApprove)
								.add("pctDisapprove", pctDisapprove))     
							.build();			
		}		
		else {
			finalStruct = Json.createObjectBuilder().add("status","404").build();			
		}		
		return finalStruct;		
	}
	
	private JsonObject get_gd(String companyName, String country) {
		return gd_driver(url_gd + companyName + "&country=" + country);
	}
	
	private JsonObject get_gd(String companyName) {
		return gd_driver(url_gd + companyName);
	}
	
	private JsonObject get_fc(String companyName) {
		String url = url_fc + companyName + ".com";
        Response r_fc = client.target(url).request(MediaType.APPLICATION_JSON).header("X-FullContact-APIKey", fc_api_key).get(Response.class);
        int responseCode = r_fc.getStatus();        
        JsonObject finalStruct = Json.createObjectBuilder().add("status", responseCode).build();        
        if(responseCode == 200) {
	        String fc = r_fc.readEntity(String.class);	 
			JsonReader jsonReader_fc = Json.createReader(new StringReader(fc));
			JsonObject jobj_fc = jsonReader_fc.readObject(); 
			JsonObject addr = (JsonObject) jobj_fc.getJsonObject("organization").getJsonObject("contactInfo").getJsonArray("addresses").get(0);		
			JsonArray phno = jobj_fc.getJsonObject("organization").getJsonObject("contactInfo").getJsonArray("phoneNumbers");
			JsonArray kw = jobj_fc.getJsonObject("organization").getJsonArray("keywords");		
			JsonString name = jobj_fc.getJsonObject("organization").getJsonString("name");
			JsonNumber numEmp = jobj_fc.getJsonObject("organization").getJsonNumber("approxEmployees");
			JsonString founded = jobj_fc.getJsonObject("organization").getJsonString("founded");
			JsonString overview = jobj_fc.getJsonObject("organization").getJsonString("overview");
			JsonString website = jobj_fc.getJsonString("website");
			JsonArray socProf = jobj_fc.getJsonArray("socialProfiles");
			
			finalStruct = Json.createObjectBuilder()
					.add("organization", Json.createObjectBuilder()
							.add("name", name)
							.add("approxEmployees", numEmp)
							.add("founded", founded)
							.add("overview", overview)
							.add("contactInfo",Json.createObjectBuilder() 
									.add("phoneNumbers", phno) 
									.add("address", addr))
							.add("keywords", kw))
					.add("website", website)
					.add("socialProfiles", socProf)
				    .build();
        	}	
		return finalStruct;		
	}
	

	/* TODO:
	 * Change return null; to an appropriate response object
	 * the response object is of the JSON response obtained from the database.
	 * the response details are in the database service's API spec.
	 * look at the database operation being carried to identify the response's contents from the API spec.
	 */
	@POST
	@Path("/post/company/add")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addCompany(@FormParam("companyName") String companyName) throws InterruptedException, ExecutionException {
		Map<String, Object> row = new HashMap<String, Object>();
		List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
		row.put("name", companyName);
		row.put("id", new Integer(companyName.hashCode()).toString()); /* assumed to be unique */
		rows.add(row);
		Response response = database.insert("Company", rows).get();
		return null;
	}
	
	@POST
	@Path("/post/university/add")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addUniversity(@FormParam("universityName") String universityName) throws InterruptedException, ExecutionException {
		Map<String, Object> row = new HashMap<String, Object>();
		List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
		row.put("name", universityName);
		row.put("id", new Integer(universityName.hashCode()).toString()); /* assumed to be unique */
		rows.add(row);
		Response response = database.insert("University", rows).get();
		return null;
	}
	
	@POST
	@Path("/post/recruitmentVisit/add")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addRecruitmentVisit(@FormParam("companyID") String companyID,
										@FormParam("universityID") String universityID,
										@FormParam("year") Integer year,
										@FormParam("gpaCutoff") Float gpaCutOff,
										@FormParam("remuneration") Integer remuneration) throws InterruptedException, ExecutionException {
		Map<String, Object> row = new HashMap<String, Object>();
		List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
		row.put("companyID", companyID);
		row.put("universityID", universityID);
		row.put("year", year);
		row.put("gpaCutoff", gpaCutOff);
		row.put("remuneration", remuneration);
		rows.add(row);
		Response response = database.insert("CompanyRecruitmentVisit", rows).get();
		return null;
	}
	
	@PUT
	@Path("/update")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.APPLICATION_JSON)
	public Response updateDetails(@FormParam("companyID") String companyID,
							      @FormParam("universityID") String universityID,
							      @FormParam("year") Integer year,
							      @DefaultValue("-1.0f") @FormParam("gpaCutoff") Float gpaCutOff,
							      @DefaultValue("-1") @FormParam("remuneration") Integer remuneration) throws InterruptedException, ExecutionException {
		if(gpaCutOff < 0.0f && remuneration == -1)
			throw new BadRequestException("Should provide at least one of the optional parameters");
		Map<String, Object> setClause = new HashMap<String, Object>();
		if(!(gpaCutOff < 0.0f))
			setClause.put("gpaCutoff", gpaCutOff);
		if(remuneration != -1)
			setClause.put("remuneration", remuneration);
		Map<String, Object> whereClause = new HashMap<String, Object>();
		whereClause.put("companyID", companyID);
		whereClause.put("universityID", universityID);
		whereClause.put("year", year);
		Response response = database.update("CompanyRecruitmentVisit", setClause, Optional.of(whereClause)).get();
		return null;
	}	
	
	@POST
	@Path("/post/studentReviews")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addStudentReview( @FormParam("companyID") String companyID,
			                          @FormParam("universityID") String universityID,
			                          @FormParam("studentName") String studentName,
			                          @FormParam("review") String review) throws InterruptedException, ExecutionException {
		Map<String, Object> row = new HashMap<String, Object>();
		List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
		row.put("companyID", companyID);
		row.put("universityID", universityID);
		row.put("studentName", studentName);
		row.put("review", review);
		rows.add(row);
		Response response = database.insert("StudentReviews", rows).get();
		return null;
	}
	
	@GET
	@Path("/get/studentReviews")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getStudentReviews(@QueryParam("companyID") String companyID,
			                          @DefaultValue("") @QueryParam("universityID") String universityID,
			                          @DefaultValue("10") @QueryParam("limit") Integer limit) throws InterruptedException, ExecutionException {
		
		List<String> tables = new ArrayList<String>();
		tables.add("Company company");
		tables.add("University university");
		tables.add("StudentReviews studentReviews");
		List<String> columns = new ArrayList<String>();
		String joinCondition = "company.id=studentReviews.companyID";
		columns.add("company.name");
		if(universityID.equals("")) {
			columns.add("university.name");
			joinCondition += " AND university.id=studentReviews.universityID";             
		}
		columns.add("studentReviews.name");
		columns.add("studentReviews.review");
		Response response = database.select(tables, columns, Optional.of(joinCondition), Optional.of(limit)).get();
		return null;
	}
	
	@DELETE
	@Path("/delete/company")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteCompany(@QueryParam("companyID") String companyID) {
		Map<String, Object> whereClauseMainTable = new HashMap<String, Object>();
		whereClauseMainTable.put("id", companyID);
		Map<String, Object> whereClause = new HashMap<String, Object>();
		whereClause.put("companyID", companyID);
		database.delete("University", Optional.of(whereClauseMainTable));
		database.delete("CompanyRecruitmentVisit", Optional.of(whereClause));
		database.delete("StudentReviews", Optional.of(whereClause));		
		return null;
	}
	
	@DELETE
	@Path("/delete/university")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteUniversity(@QueryParam("universityID") String universityID) {
		Map<String, Object> whereClauseMainTable = new HashMap<String, Object>();
		whereClauseMainTable.put("id", universityID);
		Map<String, Object> whereClause = new HashMap<String, Object>();
		whereClause.put("universityID", universityID);
		database.delete("University", Optional.of(whereClauseMainTable));
		database.delete("CompanyRecruitmentVisit", Optional.of(whereClause));
		database.delete("StudentReviews", Optional.of(whereClause));		
		return null;
	}
}
