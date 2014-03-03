/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft;


/**
 * Base interface for all members of a RAFT cluster. A Server is always in
 * either the role of Follower, Candidate or Leader. There can only be one
 * Leader active at any given point in time.
 */
public interface Server {

	/**  Tells this {@code Server} to start running. */
	void start();

	/**  Tells this {@code Server} to stop running. */
	void shutdown();

	/**
	 * Returns this server's unique identifier.
	 *
	 * @return the identifier for this {@code Server}
	 */
	long getId();

	/**
	 * Invokes this {@code Server} with the given message.
	 *
	 * @param message a {@code Message} representing a procedure to run
	 *
	 * @throws IllegalArgumentException if the message type is unknwon
	 */
	void invoke(Message message);

	/**
	 * Sends a command to the replicated log through this {@code Server}.
	 * Depending on the implementation, if this {@code Server} is not the
	 * {@code LEADER} then this message may either be rejected or forwarded
	 * on to another member.
	 *
	 * @param command the {@code Command} to append to the replicated log
	 */
	void send(Command command);

	/**
	 * Sends a command to the replicated log through this {@code Server},
	 * notifying the listener of the result. Depending on the implementation,
	 * if this {@code Server} is not the {@code LEADER} then this message may
	 * either be rejected or forwarded on to another member.
	 *
	 * @param command the {@code Command} to append to the replicated log
	 * @param listener a {code CommandResultListener} to notify with the result
	 *                 of attempting to append the command
	 */
	void send(Command command, CommandResultListener listener);

}
