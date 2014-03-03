/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.util;

import java.util.Map;
import java.util.HashMap;

import net.purpleclay.raft.ConsensusHandler;
import net.purpleclay.raft.MembershipHandle;


/**
 * Basic implementation of {@code ConsensusHandler} that uses simple majority
 * as the consensus metric. This implements the functionality described in
 * the RAFT paper, using cluster majority and tracking the match identifier
 * for each server.
 */
public class MajorityConsensusHandler implements ConsensusHandler {

	// handle to the cluster membership
	private final MembershipHandle membershipHandle;

	// map from server identifier to the last ack'd append index in the term
	private final Map<Long,Long> matchMap = new HashMap<Long,Long>();

	// the vote tally for the most recent (or active) election
	private int electionTally = 0;

	// the term of the most recent (or active) election
	private long electionTerm = 0L;

	/**
	 * Creates an instance of {@code MajorityConsensusHandler}.
	 *
	 * @param membershipHandle the {@code MembershipHandle} for the cluster
	 */
	public MajorityConsensusHandler(MembershipHandle membershipHandle) {
		this.membershipHandle = membershipHandle;
	}

	/* Implement ConsensusHandler */

	@Override public void updateTerm(long term) {
		matchMap.clear();
	}

	@Override public long appended(long senderId, long matchIndex,
								   long currentCommitIndex)
	{
		// ignore updates that are behind the current index
		if (matchIndex <= currentCommitIndex)
			return 0L;

		long appliedIndex = 0L;
		matchMap.put(senderId, matchIndex);

		// starting from the commit index work forward to the match index
		// to see if this call results in one or more entries being applied

		for (long i = currentCommitIndex + 1L; i <= matchIndex; i++) {
			int count = 0;
			for (long index : matchMap.values()) {
				if (index >= i) {
					if (isMajority(++count)) {
						appliedIndex = i;
						break;
					}
				}
			}

			if (appliedIndex < i)
				break;
		}

		return appliedIndex;
	}

	@Override public boolean receivedVote(long senderId, long term) {
		if (term < electionTerm)
			return false;

		if (term > electionTerm) {
			electionTerm = term;
			electionTally = 0;
		}

		return isMajority(++electionTally);
	}

	/** Simple majority calculation. */
	private boolean isMajority(int count) {
		return count > (membershipHandle.getMembershipCount() / 2.0);
	}

}
