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

import java.util.Properties;

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

public class BrokerReceiver {
	
	//private static final int DEFAULT_COUNT = 10;
	final String INITIAL_CONTEXT_FACTORY = "org.apache.qpid.jms.jndi.JmsInitialContextFactory";
	final String CONNECTION_JNDI_NAME = "remote";
	final String CONNECTION_NAME = "amqp://localhost:30007";
	final String QUEUE_JNDI_NAME = "queue";
	final String QUEUE_NAME = "test.queue";
	
	private Context context;
	private ConnectionFactory factory;
	private Destination queue;
	private Connection connection;
	private Session session;

	public BrokerReceiver() throws Exception{
		Properties properties = new Properties();
		properties.put(Context.INITIAL_CONTEXT_FACTORY, INITIAL_CONTEXT_FACTORY);
		properties.put("connectionfactory." + CONNECTION_JNDI_NAME , CONNECTION_NAME);
		properties.put("queue." + QUEUE_JNDI_NAME , QUEUE_NAME);
		
		this.context = new InitialContext();

		this.factory = (ConnectionFactory) context.lookup("myFactoryLookup");
		//this.factory = (ConnectionFactory) context.lookup(CONNECTION_JNDI_NAME);
		this.queue = (Destination) context.lookup("myQueueLookup");
		//this.queue = (Destination) context.lookup(QUEUE_JNDI_NAME);

		this.connection = factory.createConnection(System.getProperty("USER"), System.getProperty("PASSWORD"));
		this.connection.setExceptionListener(new MyExceptionListener());
		this.connection.start();

		this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	}

	public void receive(int nbmsgs) {
		int count = nbmsgs;
		try {
			MessageConsumer messageConsumer = this.session.createConsumer(queue);

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
