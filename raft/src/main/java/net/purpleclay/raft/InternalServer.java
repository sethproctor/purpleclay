/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft;

import net.purpleclay.raft.client.Server;


/**
 * Internal interface for all members of a RAFT cluster. An {@code InternalServer}
 * is used within the raft implementation (and tests) but should not be used by
 * an external client.
 */
public interface InternalServer extends Server {

	/**
	 * Invokes this {@code InternalServer} with the given message.
	 *
	 * @param message a {@code Message} representing a procedure to run
	 *
	 * @throws IllegalArgumentException if the message type is unknwon
	 */
	void invoke(Message message);

}
