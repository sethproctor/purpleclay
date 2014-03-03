/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft;


/**
 * Interface for all commands that can be coordinated. For each command sent
 * sent to a {@code Server} there must be some associated {@code StateMachine}
 * available to process the command.
 * <p>
 * TODO: in the current tests implementations are assumed to be VM-local or
 * {@code Serializable}. This interface needs to define some way to encode and
 * decode commands to allow for other forms of communications.
 */
public interface Command {

	/**
	 * The identifier for this command.
	 *
	 * @return a {@code String} identifying this command
	 */
	String getIdentifier();

}
