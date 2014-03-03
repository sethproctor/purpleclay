/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.rill.server;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListSet;

import net.purpleclay.rill.Endpoint;
import net.purpleclay.rill.EndpointListener;
import net.purpleclay.rill.EndpointStreamHandler;


class NetworkEndpoint implements Endpoint {

	private final SocketChannel channel;
	private final InetAddress address;

	private final EndpointStreamHandler streamHandler;

	private final int BUF_LEN = 1024 * 16;
	private final ByteBuffer outputBuffer = ByteBuffer.allocateDirect(BUF_LEN);

	private final Collection<EndpointListener> listeners =
		new ConcurrentSkipListSet<EndpointListener>();

	NetworkEndpoint(SocketChannel channel, EndpointStreamHandler streamHandler) {
		this.channel = channel;
		this.address = channel.socket().getInetAddress();

		this.streamHandler = streamHandler;
	}

	@Override public void addListener(EndpointListener listener) {
		listeners.add(listener);
	}

	@Override public void removeListener(EndpointListener listener) {
		listeners.remove(listener);
	}

	@Override public synchronized void send(byte [] message) throws IOException {
		/*
		for (int i = 0; i < message.length; i += BUF_LEN) {
			outputBuffer.clear();
			int len = (i + BUF_LEN > message.length) ? BUF_LEN : message.length - i;
			outputBuffer.put(message, i, len);
			channel.write(outputBuffer);
		}
		*/
		outputBuffer.clear();
		streamHandler.encodeOutput(message, outputBuffer);
		outputBuffer.rewind();
		channel.write(outputBuffer);
	}

	@Override public boolean send(byte [] message,
								  EndpointListener responseListener)
	{
		return false;
	}

	@Override public byte [] sendAndReceive(byte [] message) {
		return null;
	}

	@Override public void close() {
		
	}

	/*  */

	@Override public boolean equals(Object other) {
		if (! (other instanceof NetworkEndpoint))
			return false;
		return address.equals(((NetworkEndpoint) other).address);
	}

	@Override public int hashCode() {
		return address.hashCode();
	}

	/*  */

	void notifyMessage(byte [] message) {
		for (EndpointListener listener : listeners)
			listener.messageReceived(message);
	}

	void notifyClosed() {
		for (EndpointListener listener : listeners)
			listener.disconnected();
	}

}
