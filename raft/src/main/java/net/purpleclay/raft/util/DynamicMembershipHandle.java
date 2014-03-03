/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.util;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.purpleclay.raft.Command;
import net.purpleclay.raft.CommandResultListener;
import net.purpleclay.raft.MembershipHandle;
import net.purpleclay.raft.Message;
import net.purpleclay.raft.Server;
import net.purpleclay.raft.StateMachine;


/**
 * Utility class for handling server membership changes. This class implements
 * {@code MembershipHandle} to provide access to the complete membership. It
 * also implements {@code StateMachine} so that membership changes can be
 * made via the distributed log. Typically, instances are used as the handle
 * for cluster membership and added as one of several state machines via a
 * utility like {@code DelegatingStateMachine} registered using the identifier
 * {@code COMMAND_ID}.
 * <p>
 * State changes can and will happen independently of when a cluster member
 * tries to join or leave. Because of this, the model supported is that once a
 * member is added via a {@code Command} created by {@code createAddCommand} it
 * stays in the membership until an associated {@code createRemoveCommand}
 * {@code Command} is sent. While a server is in the membership it will be
 * unavailable for any RPCs or Command-handling until some specific instance
 * of {@code Server} is registered. For instance, a common pattern is to
 * send the add membership command along with registering a {@code LocalServer}.
 * The ordering of these two operations is unimportant.
 * <p>
 * Note that this implementation will call {@code start} on any registered
 * {@code Server} instance once it is associated with a valid member.
 * <p>
 * TODO: if there was some object that could be associated with each server
 * when added then it would be easy to advertise (and persist) things like
 * address or host name.
 */
public class DynamicMembershipHandle implements MembershipHandle, StateMachine {

	/** Stable identifier for the commands used by this {@code StateMachine}. */
	public static final String COMMAND_ID = "DynamicMembershipHandle";

	// the servers applied to the replicated log as the accepted membership
	private final ConcurrentMap<Long,Server> servers =
			new ConcurrentHashMap<Long,Server>();

	// the instances that have been registered but aren't in the membership
	private final Map<Long,Server> pending = new HashMap<Long,Server>();
	
	/* Implement MembershipHandle */

	@Override public int getMembershipCount() {
		return servers.size();
	}

	@Override public void invokeAll(Message message) {
		for (Server server : servers.values()) {
			if (server.getId() != message.getSenderId())
				server.invoke(message);
		}
	}

	@Override public Server findServer(long id) {
		Server server = servers.get(id);
		if (server != null)
			return server;

		// when a server first joins it will communicate with the leader before
		// the local replicated log has replayed the leader into the membership
		// so make any pending servers available, but only by direct lookup

		synchronized (pending) {
			return pending.get(id);
		}
	}

	@Override public Collection<Server> getServers() {
		return servers.values();
	}

	/* Implement StateMachine */

	@Override public void apply(Command command) {
		if (! (command instanceof MembershipCommand))
			throw new IllegalArgumentException("Unknown message type");
		MembershipCommand mh = (MembershipCommand) command;

		// make sure that if there's any pending registration it's removed at
		// this point, and if this is a new server for the membership get
		// some instance into the map, making sure the server gets started

		synchronized (pending) {
			Server server = pending.remove(mh.serverId);
			if (mh.action == MembershipCommand.Action.ADD) {
				if (server == null)
					server = new UnavailableServer(mh.serverId);
				servers.put(mh.serverId, server);
				server.start();
			}
		}

		// if this is a removal, then simply remove from the map

		if (mh.action == MembershipCommand.Action.REMOVE)
			servers.remove(mh.serverId);
	}

	/* Public utility routines */

	/**
	 * Creates a {@code Command} for adding a server to the membership.
	 *
	 * @param serverId the unique identifier for the server to add
	 * 
	 * @return the {@code Command} to send to the leader
	 */
	public static Command createAddCommand(long serverId) {
		return new MembershipCommand(MembershipCommand.Action.ADD, serverId);
	}

	/**
	 * Creates a {@code Command} for removing a server to the membership.
	 *
	 * @param serverId the unique identifier for the server to remove
	 * 
	 * @return the {@code Command} to send to the leader
	 */
	public static Command createRemoveCommand(long serverId) {
		return new MembershipCommand(MembershipCommand.Action.REMOVE, serverId);
	}

	/**
	 * Registers the given {@code Server} instance as the active instance. If
	 * the server with this identifier is already a member then calling
	 * {@code findServer()} for this identifier will return the provided instance.
	 * Otherwise this instance is held until the server is verified as part of
	 * the membership via the distributed log. In either case, when this instance
	 * is promoted into the active membership {@code start} is called on it.
	 *
	 * @param server the {@code Server} to register for the server's identifier
	 */
	public final void registerServer(Server server) {
		synchronized (pending) {
			if (servers.replace(server.getId(), server) == null)
				pending.put(server.getId(), server);
			else
				server.start();
		}
	}

	/**
	 * De-registers any previous association between a specific {@code Server}
	 * instance and the given server identifier. This does not change the verified
	 * membership, it only changes the specific instance for the identifier.
	 *
	 * @param serverId the identifier for the server to de-register
	 */
	public final void deregisterServer(long serverId) {
		synchronized (pending) {
			if (pending.remove(serverId) == null)
				servers.replace(serverId, new UnavailableServer(serverId));
		}
	}

	/** Private Command implementation for membership management. */
	private static class MembershipCommand implements Command, Serializable {
		private static final long serialVersionUID = -3973244826383976057L;
		enum Action { ADD, REMOVE };
		final Action action;
		final long serverId;
		MembershipCommand(Action action, long serverId) {
			this.action = action;
			this.serverId = serverId;
		}
		@Override public String getIdentifier() {
			return COMMAND_ID;
		}
	}

	/** Private Server implementation for members that aren't registered. */
	private static class UnavailableServer implements Server {
		private final long serverId;
		UnavailableServer(long serverId) { this.serverId = serverId; }
		@Override public void start() {  }
		@Override public void shutdown() {  }
		@Override public long getId() { return serverId; }
		@Override public void invoke(Message message) {  }
		@Override public void send(Command command) {  }
		@Override public void send(Command command, CommandResultListener listener) {
			listener.commandFailed();
		}
	}
	
}
