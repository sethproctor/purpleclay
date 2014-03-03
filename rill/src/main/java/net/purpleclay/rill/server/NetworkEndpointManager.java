/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.rill.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import net.purpleclay.rill.ConnectionHandler;
import net.purpleclay.rill.Endpoint;
import net.purpleclay.rill.EndpointManager;
import net.purpleclay.rill.EndpointStreamHandler;


public class NetworkEndpointManager implements EndpointManager {

	private final Selector selector;
	private final ServerSocketChannel serverChannel;

	private final EndpointStreamHandler streamHandler;

	private final ExecutorService executor;

	public NetworkEndpointManager(int port, EndpointStreamHandler streamHandler)
		throws IOException
	{
		this.selector = Selector.open();
		this.serverChannel = ServerSocketChannel.open();

		serverChannel.socket().bind(new InetSocketAddress(port));
		serverChannel.configureBlocking(false);
		serverChannel.register(selector, SelectionKey.OP_ACCEPT);

		this.streamHandler = streamHandler;

		this.executor = Executors.newFixedThreadPool(1);
	}

	@Override public void startServer(ConnectionHandler connectionHandler) {
		executor.execute(new ServerRunnable(connectionHandler));
	}

	@Override public void shutdown() {
		executor.shutdown();

		try {
			serverChannel.socket().close();
			serverChannel.close();
			selector.close();
		} catch (IOException ioe) {
			System.out.println(ioe.getMessage());
		}
	}

	@Override public Endpoint open(InetSocketAddress address) throws IOException {
		SocketChannel channel = SocketChannel.open();
		channel.configureBlocking(false);
		channel.connect(address);
		channel.finishConnect();

		Endpoint endpoint = new NetworkEndpoint(channel, streamHandler);
		selector.wakeup();
		channel.register(selector, SelectionKey.OP_READ).attach(endpoint);

		return endpoint;
	}

	private class ServerRunnable implements Runnable {
		private static final int BUF_LEN = 1024 * 16;
		private final ByteBuffer inputBuffer = ByteBuffer.allocateDirect(BUF_LEN);
		private final ConnectionHandler connectionHandler;
		ServerRunnable(ConnectionHandler connectionHandler) {
			this.connectionHandler = connectionHandler;
		}
		@Override public void run() {
			while (true) {
				try {
					if (selector.select() == 0)
						Thread.sleep(100L);
				} catch (IOException ioe) {
					ioe.printStackTrace();
					return;
				} catch (InterruptedException ie) {
					return;
				}

				for (SelectionKey key : selector.selectedKeys()) {
					if (key.isValid()) {
						try {
							if (key.isAcceptable()) {
								accept(key);
							} else if (key.isReadable()) {
								read(key);
							}
						} catch (IOException ioe) {
							System.out.println(ioe.getMessage());
							close(key);
						}
					} else {
						close(key);
						key.cancel();
					}
				}
			}
		}
		private void accept(SelectionKey key) throws IOException {
			SocketChannel channel = serverChannel.accept();
			if (channel == null)
				return;

			channel.configureBlocking(false);
			channel.socket().setTcpNoDelay(true);
			channel.finishConnect();
			channel.register(selector, SelectionKey.OP_READ);

			Endpoint endpoint = new NetworkEndpoint(channel, streamHandler);
			key.attach(endpoint);

			connectionHandler.newConnection(endpoint);
		}
		private void read(SelectionKey key) throws IOException {
			inputBuffer.clear();
			int len = ((SocketChannel) key.channel()).read(inputBuffer);
			if (len < 0)
				throw new IOException("Connection was closed");

			inputBuffer.rewind();
			byte [] message =
				streamHandler.handleInput((Endpoint) key.attachment(), inputBuffer);
			if (message != null) {
				// TODO: needs to be handed off to some kind of queue..
				((NetworkEndpoint) key.attachment()).notifyMessage(message);
			}
		}
		private void close(SelectionKey key) {
			try {
				key.channel().close();
			} catch (IOException ioe) {
				System.out.println(ioe.getMessage());
			}
			// TODO: needs to be handed off to some kind of queue..
			((NetworkEndpoint) key.attachment()).notifyClosed();
		}
	}

}
