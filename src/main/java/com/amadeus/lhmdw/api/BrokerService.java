package com.amadeus.lhmdw.api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import com.amadeus.lhmdw.core.BrokerSender;

@Path("/brokerservice")
public class BrokerService {
	
	@SuppressWarnings("finally")
	@Path("{msg}")
	@GET
	@Produces("application/xml")
	public String sendMessage(@PathParam("msg") String message){
		String result = "";
		try{
			BrokerSender sender = new BrokerSender(); 
			sender.send(message);
			result = "<broker><message>" + message + "</message></broker>";
		} catch(Exception e){
			result = "<error><message>" + e.getMessage() + "</message></error>";
		} finally{
			return result;
		}
	}

}
