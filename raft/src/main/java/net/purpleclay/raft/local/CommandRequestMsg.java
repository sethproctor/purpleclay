/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.local;

import net.purpleclay.raft.Command;


/** Request to append a command to the distributed log. */
class CommandRequestMsg extends AbstractMessage {

	/** Stable identifier for this message type. */
	static final String IDENTIFIER = "CommandRequest";

	/** standard serialization version identifier */
	public static final long serialVersionUID = 1L;

	// the command to append
	private final Command command;

	// some identifier that can be used to correlate responses
	private final long requestId;

	/**
	 * Creates an instance of {@code CommandRequestMsg}.
	 * 
	 * @param senderId the sender's unique identifier
	 * @param term the sender's current term
	 * @param command the command to append to the log
	 */
	CommandRequestMsg(long senderId, long term, Command command) {
		this(senderId, term, command, -1L);
	}

	/**
	 * Creates an instance of {@code CommandRequestMsg} with an associated
	 * identifier that can be used to match responses.
	 * 
	 * @param senderId the sender's unique identifier
	 * @param term the sender's current term
	 * @param command the command to append to the log
	 * @param requestId a unique identifier for matching responses
	 */
	CommandRequestMsg(long senderId, long term, Command command, long requestId) {
		super(senderId, term, IDENTIFIER);

		this.command = command;
		this.requestId = requestId;
	}

	/**
	 * Returns the {@code Command} to apply.
	 * 
	 * @return the command to apply
	 */
	Command getCommand() {
		return command;
	}

	/**
	 * Returns whether or not a response has been requested by the sender.
	 * 
	 * @return {@code true} if the sender requested a response to this request
	 *         {@code false} otherwise
	 */
	boolean isResponseRequested() {
		return requestId != -1L;
	}

	/**
	 * Returns the request identifier.
	 * 
	 * @return the unique idenifier for this request
	 *
	 * @throws IllegalStateException if no identifier was supplied
	 */
	long getRequestId() {
		if (requestId == -1L)
			throw new IllegalStateException("no identifier was included");
		return requestId;
	}

}
