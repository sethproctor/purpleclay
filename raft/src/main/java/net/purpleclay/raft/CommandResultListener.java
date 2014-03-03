/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft;


/**
 * Callback interface associated with the request to apply some command. Note
 * that because RAFT clusters may continue to run even if a majority is not
 * available a command may be indefinitely delayed so this interface
 * represents a best-effort notification.
 * <p>
 * TODO: should this include the original {@code Command}?
 */
public interface CommandResultListener {

	/**
	 * Notifies the listener that some {@code Command} was successfully applied
	 * (committed) to the distributed state machine.
	 */
	void commandApplied();

	/**
	 * Notifies the listener that some {@code Command} could not be applied
	 * (committed) to the distributed state machine and was rejected.
	 * <p>
	 * TODO: this should probably include some status for why (if known) the
	 * command was rejected.
	 */
	void commandFailed();

}
