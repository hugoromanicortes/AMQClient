package com.amadeus.lhmdw.test.core;

import com.amadeus.lhmdw.core.BrokerReceiver;

public class TestBrokerReceiver {
	private static final int DEFAULT_COUNT = 10;

	public static void main(String[] args) throws Exception {
		int count = DEFAULT_COUNT;
		BrokerReceiver receiver = new BrokerReceiver();

		if (args.length == 0) {
			System.out.println("Consuming up to " + count + " messages.");
			System.out.println(
					"Specify a message count as the program argument if you wish to consume a different amount.");
		} else {
			count = Integer.parseInt(args[0]);
			System.out.println("Consuming up to " + count + " messages.");
		}
		receiver.receive(count);

	}
}
