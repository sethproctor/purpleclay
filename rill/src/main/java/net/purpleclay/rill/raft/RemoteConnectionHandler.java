/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.rill.raft;

import net.purpleclay.raft.util.DynamicMembershipHandle;
import net.purpleclay.rill.ConnectionHandler;
import net.purpleclay.rill.Endpoint;
import net.purpleclay.rill.EndpointListener;


/**
 * Implementation of {@code ConnectionHandler} that listens for incoming
 * server connections and maps them into the memberhsip.
 */
class RemoteConnectionHandler implements ConnectionHandler {

	private final DynamicMembershipHandle membershipHandle;

	RemoteConnectionHandler(DynamicMembershipHandle membershipHandle) {
		this.membershipHandle = membershipHandle;
	}

	@Override public void newConnection(Endpoint endpoint) {
		endpoint.addListener(new EndpointListenerImpl(endpoint));
	}

	/**  */
	private class EndpointListenerImpl implements EndpointListener {
		private final Endpoint endpoint;
		EndpointListenerImpl(Endpoint endpoint) {
			this.endpoint = endpoint;
		}
		@Override public void messageReceived(byte [] message) {
			// TOOO: this could have a leading field that identified which
			// class of message (server, client, etc.) this is, but for now
			// we'll just assume that all messages are RAFT-core

			RemoteServer.register(message, endpoint, membershipHandle);
			endpoint.removeListener(this);
		}
		@Override public void disconnected() { }
	}

}
