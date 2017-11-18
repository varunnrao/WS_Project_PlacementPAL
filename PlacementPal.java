package com.vnr;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/")
public class PlacementPal 
{
	
	String url_gd = "http://api.glassdoor.com/api/api.htm?t.p=208265&t.k=cjGeqw2LUI&userip=0.0.0.0&useragent=&format=json&v=1&action=employers&q=";
	String url_fc = "https://api.fullcontact.com/v2/company/lookup.json?domain=";
	final String fc_api_key = "ff7d5aaa42c5cee1";
	Client client = ClientBuilder.newClient();

	@Context
	private ServletContext sctx;
	
/*
 * TODO : 
 * - Fix get_all by re-building the structure as per API documentation, after calling get_gd and get_fc - DONE
 * - Error Checking for both get_gd and get_fc; Add another 'status' field to the JsonObject that is being returned.
 * 			-> GlassDoor does not clearly indicate a 400; A pseudo-indicator is 'totalRecordCount' field of the 'response' field being equal to 0
 * 			-> "status" is 404 in case of FullContact
 * - Overload the get_gd fn to take a city/country parameter
 * 			-> Still need to figure out how to differentiate between a country and city as the url patterns will be identical
 * - Test each endpoint; Some might throw exceptions now due to the code re-factoring
 * */	
	

	@GET
	@Path("/get/all/{companyName: [a-zA-Z]+}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getAllJSON(@PathParam("companyName") String companyName)
	{
		
		//populate(companyName)	;
		
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
	public Response getRatings(@PathParam("companyName") String companyName)
	{
		JsonObject fs = get_gd(companyName)	;	
		//System.out.println(fs);
		return Response.ok(fs.getJsonObject("ratings").toString(),MediaType.APPLICATION_JSON).build();
		
	}
	
	@GET
	@Path("/get/contact/{companyName: [a-zA-Z]+}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getContact(@PathParam("companyName") String companyName)
	{
		JsonObject fs = get_fc(companyName)	;		
		
		JsonObject addrAndPhNo = fs.getJsonObject("organization").getJsonObject("contactInfo");
		JsonString website = fs.getJsonString("website");
		JsonObject contactInfo = Json.createObjectBuilder()
				.add("Contact Details", addrAndPhNo)
				.add("Website", website)
			.build();
		return Response.ok(contactInfo.toString(),MediaType.APPLICATION_JSON).build();
		
	}
	
	@GET
	@Path("/get/overview/{companyName: [a-zA-Z]+}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getOverview(@PathParam("companyName") String companyName)
	{
		JsonObject fs = get_fc(companyName)	;		
		JsonNumber approxEmps = fs.getJsonObject("organization").getJsonNumber("approxEmployees");
		JsonString overviewText = fs.getJsonObject("organization").getJsonString("overview");
		JsonArray kw = fs.getJsonObject("organization").getJsonArray("keywords");
		
		JsonObject overview = Json.createObjectBuilder()
				.add("approxEmployees", approxEmps)
				.add("overview", overviewText)
				.add("keywords", kw)
			.build();
		return Response.ok(overview.toString(),MediaType.APPLICATION_JSON).build();
		
	}
	
	@GET
	@Path("/get/ceo/{companyName: [a-zA-Z]+}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getCEO(@PathParam("companyName") String companyName)
	{
		JsonObject fs = get_gd(companyName);		
		return Response.ok(fs.getJsonObject("ceo").toString(),MediaType.APPLICATION_JSON).build();
	}
	
	@GET
	@Path("/get/social/{companyName: [a-zA-Z]+}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getSocial(@PathParam("companyName") String companyName)
	{
		JsonObject fs = get_fc(companyName);	
		return Response.ok(fs.getJsonArray("socialProfiles").toString(),MediaType.APPLICATION_JSON).build();
	}
	
	@GET
	@Path("/get/review/featured/{companyName: [a-zA-Z]+}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getReviews(@PathParam("companyName") String companyName)
	{
		JsonObject fs = get_gd(companyName);	
		return Response.ok(fs.getJsonObject("featuredReview").toString(),MediaType.APPLICATION_JSON).build();
	}
	
	
	private JsonObject get_gd(String companyName)
	{
		String url = url_gd + companyName;
        String gd_response = client.target(url).request(MediaType.APPLICATION_JSON).get(String.class);		
		JsonReader jsonReader_gd = Json.createReader(new StringReader(gd_response));
		JsonObject jobj_gd = jsonReader_gd.readObject();

	
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
	

		
		JsonObject finalStruct = Json.createObjectBuilder()
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
		
		return finalStruct;
	}
	
	private JsonObject get_fc(String companyName)
	{
		String url = url_fc + companyName + ".com";
        String fc = client.target(url).request(MediaType.APPLICATION_JSON).header("X-FullContact-APIKey", fc_api_key).get(String.class);		

        
		JsonReader jsonReader_fc = Json.createReader(new StringReader(fc));
		JsonObject jobj_fc = jsonReader_fc.readObject();        
		
		
		/*
		 * The 'status' field of the full contact response is either 200, 202, or 404 depending on success, 'queued for search', or non-existing company.
		 * The getJsonNumber does not seem to work for the 404 case
		 * This part is still buggy
		 * */
		JsonNumber x = jobj_fc.getJsonNumber("status");
		System.out.println(x);

		
		
		JsonObject addr = (JsonObject) jobj_fc.getJsonObject("organization").getJsonObject("contactInfo").getJsonArray("addresses").get(0);		
		JsonArray phno = jobj_fc.getJsonObject("organization").getJsonObject("contactInfo").getJsonArray("phoneNumbers");
		JsonArray kw = jobj_fc.getJsonObject("organization").getJsonArray("keywords");		
		JsonString name = jobj_fc.getJsonObject("organization").getJsonString("name");
		JsonNumber numEmp = jobj_fc.getJsonObject("organization").getJsonNumber("approxEmployees");
		JsonString founded = jobj_fc.getJsonObject("organization").getJsonString("founded");
		JsonString overview = jobj_fc.getJsonObject("organization").getJsonString("overview");
		JsonString website = jobj_fc.getJsonString("website");
		JsonArray socProf = jobj_fc.getJsonArray("socialProfiles");

		
		
		JsonObject finalStruct = Json.createObjectBuilder()
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
		return finalStruct;
		
	}
}