/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.local;


/** Response to a {@code CommandRequestMsg}. */
class CommandResponseMsg extends AbstractMessage {

	/** Stable identifier for this message type. */
	static final String IDENTIFIER = "CommandResponse";

	/** standard serialization version identifier */
	public static final long serialVersionUID = 1L;

	// the identifier from the associated request
	private final long requestId;

	// the index of the appended entry, or FAILED_APPEND
	private final long entryIndex;

	// constant for an unsuccessful append request
	private static final long FAILED_APPEND = -1L;
	
	/**
	 * Creates an instance of {@code CommandResponseMsg} for failed requests.
	 * 
	 * @param senderId the sender's unique identifier
	 * @param term the sender's term
	 * @param requestId the associated request identifier
	 */
	CommandResponseMsg(long senderId, long term, long requestId) {
		this(senderId, term, requestId, FAILED_APPEND);
	}

	/**
	 * Creates an instance of {@code CommandResponseMsg} for successful requests.
	 * 
	 * @param senderId the sender's unique identifier
	 * @param term the sender's term
	 * @param requestId the associated request identifier
	 * @param entryIndex the index where the command was appended
	 */
	CommandResponseMsg(long senderId, long term, long requestId, long entryIndex) {
		super(senderId, term, IDENTIFIER);

		this.requestId = requestId;
		this.entryIndex = entryIndex;
	}

	/**
	 * Returns the identifier for the associated request.
	 *
	 * @return the identifier for the associated request
	 */
	long getRequestId() {
		return requestId;
	}

	/**
	 * Returns whether or not the append request was successful.
	 * 
	 * @return {@code true} if the associated append request succeeded
	 *         {@code false} otherwise
	 */
	boolean commandAccepted() {
		return entryIndex != FAILED_APPEND;
	}

	/**
	 * Returns the index where the command was appended.
	 * 
	 * @return the index where the command was appended
	 * 
	 * @throws IllegalStateException if the request failed
	 */
	long getEntryIndex() {
		if (entryIndex == FAILED_APPEND)
			throw new IllegalStateException("no index was assigned");
		return entryIndex;
	}

	@Override public String toString() {
		return String.format("%s requestId=[%d] commandAccepted=[%b] entryIndex=[%d]", 
				super.toString(), getRequestId(), commandAccepted(), getEntryIndex());
	}
}
