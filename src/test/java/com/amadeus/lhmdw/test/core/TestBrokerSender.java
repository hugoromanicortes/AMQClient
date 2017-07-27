package com.amadeus.lhmdw.test.core;

import com.amadeus.lhmdw.core.BrokerSender;

public class TestBrokerSender {
	private static final String DEFAULT_MSG = "Hello LHMDW-2020!";
	
	public static void main(String[] args) throws Exception {
		BrokerSender sender = new BrokerSender();
		String message = DEFAULT_MSG;

		if (args.length == 0) {
			System.out.println("Sending default message.");
			System.out.println("Specify a message if you wish to send a different value.");
		} else {
			message = args[0];
			System.out.println("Sending '" + message + "' message.");
		}
		sender.send(message);
	}
}
