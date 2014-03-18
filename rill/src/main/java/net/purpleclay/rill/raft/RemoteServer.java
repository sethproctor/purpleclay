/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.rill.raft;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import net.purpleclay.raft.Command;
import net.purpleclay.raft.CommandResultListener;
import net.purpleclay.raft.MembershipHandle;
import net.purpleclay.raft.Message;
import net.purpleclay.raft.InternalServer;
import net.purpleclay.raft.util.AbstractServer;
import net.purpleclay.raft.util.DynamicMembershipHandle;
import net.purpleclay.rill.Endpoint;
import net.purpleclay.rill.EndpointListener;


/**
 * Implementation of {@code Server} that is the local interface for a remote
 * server, addressible via an {@code Endpoint}. The assumption is that each
 * VM-local server (typically with an instance of {@code LocalServer}) has an
 * instance of {@code RemoteServer} for every other member, and vica verca.
 */
public class RemoteServer extends AbstractServer {

	// the endpoint for communicating with the remote server
	private final Endpoint endpoint;

	// the local handle to the membership
	private final DynamicMembershipHandle membershipHandle;

	/**
	 * Creates an instance of {@code RemoteServer} connected to an
	 * {@code Endpoint}-addressible server in the cluster.
	 *
	 * @param id the unique identifier for the remote server
	 * @param endpoint the {@code Endpoint} for the remote server
	 * @param membershipHandle the handle to the membership
	 */
	public RemoteServer(long id, Endpoint endpoint,
					    DynamicMembershipHandle membershipHandle)
	{
		super(id);

		this.endpoint = endpoint;
		this.membershipHandle = membershipHandle;

		endpoint.addListener(new RemoteEndpointListener());
		membershipHandle.registerServer(this);
	}

	@Override public void start() {
		
	}

	@Override public void shutdown() {

	}

	@Override public void invoke(Message msg) {
		try {
			endpoint.send((new RemoteCall(msg, getId())).toBytes());
		} catch (IOException ioe) {
			// TODO: probably need some kind of invocation exception?
			System.out.println(ioe.getMessage());
		}
	}

	@Override public void send(Command command) {
		try {
			endpoint.send((new RemoteCall(command, getId())).toBytes());
		} catch (IOException ioe) {
			// TODO: probably need some kind of command exception?
			System.out.println(ioe.getMessage());
		}
	}

	@Override public void send(Command command, CommandResultListener listener) {
		// TODO: this could be supported ... but should it be?
		throw new UnsupportedOperationException("Cannot register a command listener on a remote server");
	}

	/** Register a remote server that has just connected. */
	static void register(byte [] entryMessage, Endpoint endpoint,
						 DynamicMembershipHandle membershipHandle)
	{
		// this is either a request to join or a connection from an active server
	}

	// TODO: just for testing, start with object serialization
	private static class RemoteCall implements Serializable {
		static final long serialVersionUID = 1L;
		private enum CallType { MESSAGE, COMMAND };
		private final CallType type;
		private final Object content;
		private final long targetServer;
		RemoteCall(Message message, long id) {
			this.type = CallType.MESSAGE;
			this.content = message;
			this.targetServer = id;
		}
		RemoteCall(Command command, long id) {
			this.type = CallType.COMMAND;
			this.content = command;
			this.targetServer = id;
		}
		void call(MembershipHandle membershipHandle) {
			InternalServer server = membershipHandle.findServer(targetServer);
			assert server != null : "no local server in the membership";

			if (type == CallType.MESSAGE)
				server.invoke((Message) (content));
			else
				server.send((Command) (content));
		}
		byte [] toBytes() throws IOException {
			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
			ObjectOutputStream objOut = new ObjectOutputStream(bytesOut);
			try {
				objOut.writeObject(this);
				return bytesOut.toByteArray();
			} finally {
				objOut.close();
			}
		}
		static RemoteCall fromBytes(byte [] bytes) throws IOException {
			ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytes);
			ObjectInputStream objIn = new ObjectInputStream(bytesIn);
			try {
				return (RemoteCall) objIn.readObject();
			} catch (ClassNotFoundException cnfe) {
				throw new IOException("failed to read object", cnfe);
			} finally {
				objIn.close();
			}
		}
	}

	private class RemoteEndpointListener implements EndpointListener {
		@Override public void messageReceived(byte [] message) {
			RemoteCall remoteCall = null;
			try {
				remoteCall = RemoteCall.fromBytes(message);
			} catch (IOException ioe) {
				// TODO: handle a malformed message
				System.out.println(ioe.getMessage());
				return;
			}
			remoteCall.call(membershipHandle);
		}
		@Override public void disconnected() {
			membershipHandle.deregisterServer(getId());
		}
	}

}
