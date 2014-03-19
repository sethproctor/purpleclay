/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */
package net.purpleclay.raft.local;


/** Response to a {@code VoteRequestMsg}. */
class VoteResponseMsg extends AbstractMessage {

	/** Stable identifier for this message type. */
	static final String IDENTIFIER = "VoteResponse";

	/** standard serialization version identifier */
	public static final long serialVersionUID = 1L;

	// the response to a request for a vote
	private final boolean response;

	/**
	 * Creates an instance of {@code VoteResponseMsg}.
	 * 
	 * @param senderId the sender's unique identifier
	 * @param term the term for the vote
	 * @param response whether or not the sender votes for the candidate
	 */
	VoteResponseMsg(long senderId, long term, boolean response) {
		super(senderId, term, IDENTIFIER);

		this.response = response;
	}

	/**
	 * Returns whether the sender casts their vote for the requeting candidate
	 * in the given term.
	 * 
	 * @return {@code true} if a vote was granted {@code false} otherwise
	 */
	boolean getResponse() {
		return response;
	}

	@Override public String toString() {
		return String.format("%s voteGranted=[%b]", super.toString(), getResponse());
	}

}
