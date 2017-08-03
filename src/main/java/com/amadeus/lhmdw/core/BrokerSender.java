package com.amadeus.lhmdw.core;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

public class BrokerSender {
	
	private static final int DELIVERY_MODE = DeliveryMode.NON_PERSISTENT;
	final String INITIAL_CONTEXT_FACTORY = "org.apache.qpid.jms.jndi.JmsInitialContextFactory";
	final String CONNECTION_JNDI_NAME = "remote";
	final String CONNECTION_NAME = "amqp://amq63-app-pub:5672";
	final String QUEUE_JNDI_NAME = "queue";
	final String QUEUE_NAME = "test.queue";
	
	private Context context;
	private ConnectionFactory factory;
	private Destination queue;
	private Connection connection;

	public BrokerSender() throws Exception{
		Properties properties = new Properties();
		properties.put(Context.INITIAL_CONTEXT_FACTORY, INITIAL_CONTEXT_FACTORY);
		properties.put("connectionfactory." + CONNECTION_JNDI_NAME , CONNECTION_NAME);
		properties.put("queue." + QUEUE_JNDI_NAME , QUEUE_NAME);
		
		this.context = new InitialContext(properties);
		
		//System.out.println("Name = "+ context.getEnvironment());
		//this.factory = (ConnectionFactory) context.lookup("myFactoryLookup");
		this.factory = (ConnectionFactory) context.lookup(CONNECTION_JNDI_NAME);
		//this.queue = (Destination) context.lookup("myQueueLookup");
		this.queue = (Destination) context.lookup(QUEUE_JNDI_NAME);
		
		this.connection = factory.createConnection(System.getProperty("USER"), System.getProperty("PASSWORD"));
		this.connection.setExceptionListener(new MyExceptionListener());
	}

	public void send(String msg) {
		try {
			this.connection.start();
			Session session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			MessageProducer messageProducer = session.createProducer(queue);
			
			long start = System.currentTimeMillis();
			
			TextMessage message = session.createTextMessage(msg);
			messageProducer.send(message, DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
			
			long finish = System.currentTimeMillis();
			long taken = finish - start;
			System.out.println("Sent message in " + taken + "ms");
			
			connection.close();
		} catch (Exception exp) {
			System.err.println("Caught exception, exiting.");
			exp.printStackTrace(System.err);
			//System.exit(1);
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
