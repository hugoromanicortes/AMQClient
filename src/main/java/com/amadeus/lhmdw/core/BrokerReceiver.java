/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package com.amadeus.lhmdw.core;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.qpid.jms.message.JmsTextMessage;

public class BrokerReceiver {
	private static final int DEFAULT_COUNT = 10;

	public BrokerReceiver() {
	}

	public void receive(int nbmsgs) {
		int count = nbmsgs;
		try {
			// The configuration for the Qpid InitialContextFactory has been
			// supplied in
			// a jndi.properties file in the classpath, which results in it
			// being picked
			// up automatically by the InitialContext constructor.
			Context context = new InitialContext();

			ConnectionFactory factory = (ConnectionFactory) context.lookup("myFactoryLookup");
			Destination queue = (Destination) context.lookup("myQueueLookup");

			Connection connection = factory.createConnection(System.getProperty("USER"),
					System.getProperty("PASSWORD"));
			connection.setExceptionListener(new MyExceptionListener());
			connection.start();

			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			MessageConsumer messageConsumer = session.createConsumer(queue);

			long start = System.currentTimeMillis();

			int actualCount = 0;
			boolean deductTimeout = false;
			int timeout = 1000;
			for (int i = 1; i <= count; i++, actualCount++) {
				Message message = messageConsumer.receive(timeout);
				if (message == null) {
					System.out.println("Message " + i + " not received within timeout, stopping.");
					deductTimeout = true;
					break;
				}
				if (i % 100 == 0) {
					System.out.println("Got message " + i);
				}

				System.out.println("Received message: " + ((TextMessage) message).getText());
			}

			long finish = System.currentTimeMillis();
			long taken = finish - start;
			if (deductTimeout) {
				taken -= timeout;
			}
			System.out.println("Received " + actualCount + " messages in " + taken + "ms");

			connection.close();
		} catch (Exception exp) {
			System.out.println("Caught exception, exiting.");
			exp.printStackTrace(System.out);
			System.exit(1);
		}
	}

	private static class MyExceptionListener implements ExceptionListener {
		public void onException(JMSException exception) {
			System.out.println("Connection ExceptionListener fired, exiting.");
			exception.printStackTrace(System.out);
			System.exit(1);
		}
	}
}
