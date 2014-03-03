/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.local;

import net.purpleclay.raft.Command;


/** Request to append a command (or issue a heartbeat). */
class AppendRequestMsg extends AbstractMessage {

	/** Stable identifier for this message type. */
	static final String IDENTIFIER = "AppendRequest";

	/** standard serialization version identifier */
	public static final long serialVersionUID = 1L;

	// the previous log entry details
	private final long prevLogIndex;
	private final long prevLogTerm;

	// the entries in this message, which may be empty
	private final Command [] entries;

	// the commit index at the sender (which must be the leader)
	private final long leaderCommit;

	/**
	 * Creates an instance of {@code AppendRequestMsg} with no entries, which
	 * is used as a heartbeat message.
	 * 
	 * @param senderId the sender's identifier
	 * @param term the sender's current term
	 * @param prevLogIndex the index of the previous log entry
	 * @param prevLogTerm the term of the previous log entry
	 * @param leaderCommit the current commit index
	 */
	AppendRequestMsg(long senderId, long term, long prevLogIndex,
					 long prevLogTerm, long leaderCommit)
	{
		this(senderId, term, prevLogIndex, prevLogTerm, null, leaderCommit);
	}

	/**
	 * Creates an instance of {@code AppendRequestMsg} with entries.
	 * 
	 * @param senderId the sender's identifier
	 * @param term the sender's current term
	 * @param prevLogIndex the index of the previous log entry
	 * @param prevLogTerm the term of the previous log entry
	 * @param entries the log entries to append
	 * @param leaderCommit the current commit index
	 */
	AppendRequestMsg(long senderId, long term, long prevLogIndex,
					 long prevLogTerm, Command [] entries,
					 long leaderCommit)
	{
		super(senderId, term, IDENTIFIER);

		this.prevLogIndex = prevLogIndex;
		this.prevLogTerm = prevLogTerm;
		this.leaderCommit = leaderCommit;

		this.entries = entries != null ? entries : new Command[0];
	}

	/**
	 * Returns the previous log index.
	 * 
	 * @return the previous log index
	 */
	long getPrevLogIndex() {
		return prevLogIndex;
	}

	/**
	 * Returns the previous log term.
	 * 
	 * @return the previous log term
	 */
	long getPrevLogTerm() {
		return prevLogTerm;
	}

	/**
	 * Returns the ordered set of entries in this message, which may be empty.
	 * 
	 * @return the ordered entries to append
	 */
	Command [] getEntries() {
		return entries;
	}

	/**
	 * Returns the current commit index.
	 * 
	 * @return the current commit index
	 */
	long getLeaderCommit() {
		return leaderCommit;
	}

}
