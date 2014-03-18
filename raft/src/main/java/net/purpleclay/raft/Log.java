/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft;



/**
 * Interface for the distibuted log maintained by the cluster. There is
 * typically a single instance at each {@code Server} that represents the
 * local, appended state in addition to the known, applied (committed) set
 * of commands.
 * <p>
 * TODO: this interface does not yet cover compaction, which needs to be
 * coordinated with the state machines.
 */
public interface Log {

	/**
	 * Returns the index of the most recently committed entry.
	 *
	 * @return the most recently committed entry's index
	 */
	long getCommitIndex();

	/**
	 * Returns the index of the most recently appended entry.
	 *
	 * @return the most recently appended entry's index
	 */
	long getLastIndex();

	/**
	 * Returns the term associated with the latest entry.
	 *
	 * @return the term of the latest entry
	 */
	long getLastTerm();

	/**
	 * Indicates whether there is an entry in the log at the given index that
	 * was appended in the given term.
	 *
	 * @param index the index in the log of an entry
	 * @param term the expected term for the entry
	 *
	 * @return {@code true} if the log has an entry at the given index made in
	 *         the given term or {@code false} otherwise
	 */
	boolean hasEntry(long index, long term);

	/**
	 * Returns the term in which the given entry was appended.
	 *
	 * @param index the index in the log of an entry
	 *
	 * @return the term when the given entry was appended
	 *
	 * @throws IllegalArgumentException if there is no entry at the index
	 */
	long getTerm(long index);

	/**
	 * Checks that the entry at the given index was appended in the given term.
	 * If this check fails, then this entry and all following entries are
	 * purged from the log. If the index is later than the last index then
	 * this call has no effect.
	 * <p>
	 * TODO: this should raise some exception if there's a failure
	 *
	 * @param index the index in the log of an entry
	 * @param term the expected term for when the entry was appended
	 */
	void validateEntry(long index, long term);

	/**
	 * Appends the given command to the local log in the given term. At this
	 * point the command has not been applied (committed) to the replicated
	 * state but it still must be made durable before this server acknowledges
	 * the update in case of any local failure that requires recovery.
	 * <p>
	 * TODO: this should raise some exception if there's a failure
	 *
	 * @param command a {@code Command} to append to the log
	 * @param term the term in which the command was appended
	 */
	void append(Command command, long term);

	/**
	 * Applies (commits) all commands up to and including the given index.
	 * This call indicates that consensus has been reached for the given
	 * commands so the appropriate {@code StateMachine}(s) can be notified.
	 *
	 * @param appliedIndex the most recent (inclusive) entry applied
	 */
	void applied(long appliedIndex);

	/**
	 * Returns the entries from the given index (inclusive) to the latest.
	 * <p>
	 * TODO: this was originall just here for testing, but something like this
	 * is needed for when a server has to catch-up on missed entries. Is there
	 * a cleaner way to express this that doesn't require creating arrays?
	 *
	 * @param startingIndex the starting index (inclusive) for the array
	 *
	 * @return an array of entries from the starting index to the most recent
	 */
	Command [] getEntries(long startingIndex);

}
