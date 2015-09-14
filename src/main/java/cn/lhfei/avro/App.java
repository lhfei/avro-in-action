/*
 * Copyright 2010-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.lhfei.avro;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.lhfei.avro.proto.Mail;
import cn.lhfei.avro.proto.Message;

/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Sep 10, 2015
 */
public class App {
	
	private static Server server;
	private static final Logger log = LoggerFactory.getLogger(App.class);
	
	public static class MailImpl implements Mail {
		// in this simple example just return details of the message
		public Utf8 send(Message message) {
			log.info("Sending message");
			return new Utf8("Sending message to " + message.getTo().toString()
					+ " from " + message.getFrom().toString() + " with body "
					+ message.getBody().toString());
		}
	}


	private static void startServer() throws IOException {
		server = new NettyServer(new SpecificResponder(Mail.class,
				new MailImpl()), new InetSocketAddress(65111));
		// the server implements the Mail protocol (MailImpl)
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			log.info("Usage: <to> <from> <body>");
			System.exit(1);
		}

		log.info("Starting server");
		// usually this would be another app, but for simplicity
		startServer();
		log.info("Server started");

		NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(
				65111));
		// client code - attach to the server and send a message
		Mail proxy = (Mail) SpecificRequestor.getClient(Mail.class, client);
		log.info("Client built, got proxy");

		// fill in the Message record and send it
		Message message = new Message();
		message.setTo(new Utf8(args[0]));
		message.setFrom(new Utf8(args[1]));
		message.setBody(new Utf8(args[2]));
		log.info("Calling proxy.send with message:  "
				+ message.toString());
		log.info("Result: " + proxy.send(message));

		// cleanup
		client.close();
		server.close();
	}
}
