/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft;


/**
 * Interface for all Remote Procedure Call messages sent between servers.
 * <p>
 * TODO: the current implementation uses serialization to encode and decode
 * messages that go between virtual machines. This interface should have some
 * explicit mechanism for choosing other mashalling mechanisms.
 */
public interface Message {

	/**
	 * Returns the identifier for the server that sent this message.
	 *
	 * @return the unique identifier for the sender
	 */
	long getSenderId();

	/**
	 * Returns the term when this message was sent.
	 *
	 * @return the term associated with this message
	 */
	long getTerm();

	/**
	 * Returns the identifier for this message.
	 *
	 * @return the message identifier
	 */
	String getIdentifier();

}
