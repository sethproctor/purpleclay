/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft;


/** Interface used to process replicated log commands. */
public interface StateMachine {

	/**
	 * Applies (commits) the given command to this state machine. This is
	 * called only after consensus is reached in the cluster. Calls to apply
	 * commands must be made in the order in which the commands are committed.
	 *
	 * @param command the {@code Command} to apply
	 *
	 * @throws IllegalArgumentException if the command cannot be applied to
	 *                                  this state machine implementation
	 */
	void apply(Command command);

}
