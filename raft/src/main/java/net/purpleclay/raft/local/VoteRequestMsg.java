/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.local;


/** Request to vote for a new leader. */
class VoteRequestMsg extends AbstractMessage {

	/** Stable identifier for this message type. */
	static final String IDENTIFIER = "VoteRequest";

	/** standard serialization version identifier */
	public static final long serialVersionUID = 1L;

	// the sender's last log entry index and term
	private final long lastLogIndex;
	private final long lastLogTerm;

	/**
	 * Creates an instance of {@code VoteRequestMsg}.
	 * 
	 * @param senderId the sender's unique identifier
	 * @param term the sender's current term
	 * @param lastLogIndex the sender's last log entry index
	 * @param lastLogTerm the sender's last log entry term
	 */
	VoteRequestMsg(long senderId, long term, long lastLogIndex, long lastLogTerm) {
		super(senderId, term, IDENTIFIER);

		this.lastLogIndex = lastLogIndex;
		this.lastLogTerm = lastLogTerm;
	}

	/**
	 * Returns the server identifier for the candidate requesting a vote.
	 * 
	 * @return the candidate's unique identifier
	 */
	long getCandidateId() {
		return getSenderId();
	}

	/**
	 * Returns the candidate's last log entry index.
	 * 
	 * @return the candidate's last log entry index
	 */
	long getLastLogIndex() {
		return lastLogIndex;
	}

	/**
	 * Returns the candidate's last log entry term.
	 * 
	 * @return the candidate's last log entry term
	 */
	long getLastLogTerm() {
		return lastLogTerm;
	}

	@Override public String toString() {
		return String.format("%s candidateId=[%d] lastLogIndex=[%d] lastLogTerm=[%d]", 
				super.toString(), getCandidateId(), getLastLogIndex(), getLastLogTerm());
	}

}
