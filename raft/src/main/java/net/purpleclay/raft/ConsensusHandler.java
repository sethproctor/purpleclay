/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft;


/**
 * Interface for handling the decision-making that requires cluster consensus:
 * leadership election and command acceptence. The simplest version of this is
 * the model assumed in the RAFT paper (simple majority). This interface is
 * extacted to allow for implementors to use alternate models, but care should
 * be taken to ensure that all servers are always using the same model.
 * <p>
 * It's assumed that there is a one-to-one relationship between a {@code Server}
 * and a {@code ConsensusHandler}. This means, for instance, that when a vote
 * for leadership is handled, the vote has been cast for the associated server.
 *
 * @see net.purpleclay.raft.util.MajorityConsensusHandler
 */
public interface ConsensusHandler {

	/**
	 * Notifies this handler that the current term has been updated.
	 *
	 * @perem term the new term of the cluster
	 */
	void updateTerm(long term);

	/**
	 * Notifies this handler that the given sender has appended the given
	 * entry into its local state machine. This returns the latest applied
	 * index if the notification resuts in one or more commands being applied
	 * (committed). This method may return a valid index for multiple append
	 * notifications as they arrive from each server in the cluster.
	 * <p>
	 * TODO: this works, but the use of match/current seems cumbersome and
	 * could be replaced with a single call for each command, or at least
	 * some notification like updateTerm() for the current index.
	 *
	 * @param senderId the identifier of the {@code Server} that voted
	 * @param matchIndex the highest index the sender has appended
	 * @param currentCommitIndex the current commit index in the log
	 *
	 * @return the latest applied index or 0 if no index was applied
	 */
	long appended(long senderId, long matchIndex, long currentCommitIndex);

	/**
	 * Notifies this handler that a vote for leader was heard from the given
	 * sender in an election for the given term. This returns {@code true} if
	 * the vote results in a successful campaign. This method may return
	 * {@code true} for multiple votes if they continue to be tallied after
	 * a leader has already been elected.
	 *
	 * @param senderId the identifier of the {@code Server} that voted
	 * @param term the term in which the vote was cast
	 *
	 * @return {@code true} if the vote is for a successful election in the
	 *         given term or {@code false} otherwise
	 */
	boolean receivedVote(long senderId, long term);

}
