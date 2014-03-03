/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.util;

import net.purpleclay.raft.Server;


/**
 * Utility implementation of {@code Server} that implements basic equality and
 * identity routines.
 */
public abstract class AbstractServer implements Server {

	// the identifier for this server
	private final long id;

	// cached hash value for this instance
	private final int hash;

	/**
	 * Creates an instance of {@code AbstractServer} based on an identifier.
	 *
	 * @param id the unique identifier for this {@code Server}
	 */
	protected AbstractServer(long id) {
		this.id = id;
		this.hash = Long.valueOf(id).hashCode();
	}

	/* Implement Server */

	@Override public long getId() {
		return id;
	}

	/* Implement equality */

	@Override public boolean equals(Object o) {
		if (! (o instanceof Server))
			return false;

		return ((Server) o).getId() == getId();
	}

	@Override public int hashCode() {
		return hash;
	}

}
