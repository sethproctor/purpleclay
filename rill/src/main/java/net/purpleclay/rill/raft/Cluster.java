/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.rill.raft;

import java.io.IOException;
import java.util.Properties;

import net.purpleclay.raft.Log;
import net.purpleclay.raft.Server;
import net.purpleclay.raft.StateMachine;
import net.purpleclay.raft.local.LocalServer;
import net.purpleclay.raft.util.DelegatingStateMachine;
import net.purpleclay.raft.util.DurableLog;
import net.purpleclay.raft.util.DynamicMembershipHandle;
import net.purpleclay.rill.EndpointManager;

/** Incomplete implementation for bootstrapping into a cluster. */
public class Cluster {

	private final DynamicMembershipHandle dynamicHandle =
		new DynamicMembershipHandle();

	private final DelegatingStateMachine delegatingMachine =
		new DelegatingStateMachine();

	private final Log log;

	private final EndpointManager endpointManager;

	private final Properties properties;

	public Cluster(EndpointManager endpointManager, Properties properties)
		throws IOException
	{
		this.endpointManager = endpointManager;
		this.properties = properties;

		delegatingMachine.addMachine(dynamicHandle, DynamicMembershipHandle.COMMAND_ID);

		// TODO: should the type of log be defined in the properties?
		this.log = new DurableLog(properties, delegatingMachine);
	}

	/** IllegalArgumentException */
	public void addStateMachine(StateMachine stateMachine, String commandId) {
		delegatingMachine.addMachine(stateMachine, commandId);
	}

	public void joinCluster() throws IOException {
		LocalServer server = null;

		// if there is content in the log then re-load the server state to use
		// in initial connection establishment & register the instance
		if (log.getLastIndex() != 0) {
			server = LocalServer.loadInstance(log, dynamicHandle, properties);
			dynamicHandle.registerServer(server);
		}

		// try to join the cluster, or decide that there is no active cluster
		long localId = requestEntry(server);
		boolean joinedCluster = true;
		if (localId == -1L) {
			joinedCluster = false;

			// TODO: this is the point to stop, or to continue on alone .. for
			// testing, just allow & get this server an identifier
			localId = (server == null) ? 1L : server.getId();
		}
		
		endpointManager.startServer(new RemoteConnectionHandler(dynamicHandle));

		// if the local server instance hasn't already been created then do it
		// now and get it registered into the membership
		if (server == null) {
			server = LocalServer.createInstance(localId, log, dynamicHandle, properties);
			dynamicHandle.registerServer(server);
		}

		if (! joinedCluster) {
			server.start();
			server.send(DynamicMembershipHandle.createAddCommand(localId));
		}
	}

	public void leaveCluster() {
		((DurableLog) log).shutdown();
	}

	private long requestEntry(Server localServer) throws IOException {
		// resolve & connect to leader
		// get back remote & local identifiers
		// register the remote server
		// return identifier

		// TODO: to get started testing, just allow initial startup

		return -1L;
	}

	/*
	private Server connectIfServer(InetSocketAddress address) {
		Endpoint endpoint = endpointManager.open(address);
	}
	*/

}
