/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.local;


/**
 * Response to an {@code AppendRequestMsg}. A successful response includes the
 * latest appended index, as RAFT specifies.
 * <p>
 * In the case of rejection, RAFT leaves several options open. The simplest
 * option is to return {@code false} and let the leader iterate backwards one
 * index at a time until an acceptible re-synchronization point is found. This
 * requires keeping extra state at the leader and can result in large numbers
 * of "catch-up" messages. As suggested by the RAFT authors, this message
 * implementation includes a best-guess by the follower at the index to try.
 */
class AppendResponseMsg extends AbstractMessage {

	/** Stable identifier for this message type. */
	static final String IDENTIFIER = "AppendResponse";

	/** standard serialization version identifier */
	public static final long serialVersionUID = 1L;

	/** Indication that no index was specified. */
	public static final long NO_INDEX = 0L;

	// status of the response (success or fail)
	private final boolean response;

	// the index applied or new index request, depending on the response
	private final long index;

	/**
	 * Creates an instance of {@code AppendResponseMsg} for failed requests
	 * that don't specify an explicit index to use in another attempt.
	 * 
	 * @param senderId the sender's unique identifier
	 * @param term the term of the request
	 */
	AppendResponseMsg(long senderId, long term) {
		this(senderId, term, false, NO_INDEX);
	}
	
	/**
	 * Creates an instance of {@code AppendResponseMsg} that indeicates if the
	 * associated request was successful. If it was, then the index supplied
	 * is the latest index appended at the remote server. If the request was
	 * not successful then the index is a request from the remote server to
	 * get a new append attempt at the given index.
	 * 
	 * @param senderId the sender's unique identifier
	 * @param term the term of the request
	 * @param response {@code true} if the associated request succeeded
	 *                 {@code false} otherwise
	 * @param appended the latest index appended if response is {@code true}
	 *                 or the request index to try next next otherwise
	 */
	AppendResponseMsg(long senderId, long term, boolean response, long index) {
		super(senderId, term, IDENTIFIER);

		this.response = response;
		this.index = index;
	}

	/**
	 * Returns the response.
	 * 
	 * @return {@code true} if the request succeeded {@code false} otherwise
	 */
	boolean getResponse() {
		return response;
	}

	/**
	 * Returns either the last index appended (if the response is {@code true})
	 * or the index that the leader should try next.
	 * 
	 * @return the latest index appended or the next index to try
	 */
	long getIndex() {
		return index;
	}

}
