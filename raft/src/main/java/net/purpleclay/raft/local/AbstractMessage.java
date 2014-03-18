/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.local;

import java.io.Serializable;

import net.purpleclay.raft.Message;

/** Base utility for the local server messages. */
abstract class AbstractMessage implements Message, Serializable {

	// the sender's identifier and current term
	private final long senderId;
	private final long term;

	// identifier for this message
	private final String identifier;

	/** standard serialization version identifier */
	public static final long serialVersionUID = 1L;

	/**
	 * Creates an instance of {@code AbstractMessage}.
	 * 
	 * @param senderId the sender's unique identifier
	 * @param term the sender's current term
	 * @param identifier the identifier for this message
	 */
	AbstractMessage(long senderId, long term, String identifier) {
		this.senderId = senderId;
		this.term = term;
		this.identifier = identifier;
	}

	/* Implement Message */

	@Override public long getSenderId() {
		return senderId;
	}

	@Override public long getTerm() {
		return term;
	}

	@Override public String getIdentifier() {
		return identifier;
	}

	@Override public String toString() {
		return String.format("%sMsg sender=[%d] term=[%d]", getIdentifier(), getSenderId(), getTerm());
	}
}
