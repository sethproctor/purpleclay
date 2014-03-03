/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft;

import java.util.Collection;


/** Interface for accessing the cluster membership. */
public interface MembershipHandle {

	/**
	 * Returns the current number of members. Depending on the implementation
	 * this can be the number of active servers, the total number of expected
	 * servers (as is assumed in the RAFT paper) or some other measure. In any
	 * case, it must be a consistent metric across the cluster.
	 *
	 * @return the number of members in the cluster
	 */
	int getMembershipCount();

	/**
	 * Invokes a Remote Procedure Call on all members in the cluster, excluding
	 * the sender.
	 *
	 * @param message the {@code Message} to broadcast to all other servers
	 */
	void invokeAll(Message message);

	/**
	 * Returns a {@code Server} instance matched by its unique identifier.
	 *
	 * @param id the unqiue identifier for a server
	 *
	 * @return the {@code Server} with the given identifier or {@code null} if
	 *         no server is known by the given identifier
	 */
	Server findServer(long id);

	/**
	 * Returns the known membership.
	 *
	 * @return a {@code Collection} of the known {@code Server}s
	 */
	Collection<Server> getServers();

}
