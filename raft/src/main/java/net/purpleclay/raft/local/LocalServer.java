/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.local;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import net.purpleclay.raft.Command;
import net.purpleclay.raft.CommandResultListener;
import net.purpleclay.raft.ConsensusHandler;
import net.purpleclay.raft.Log;
import net.purpleclay.raft.MembershipHandle;
import net.purpleclay.raft.Message;
import net.purpleclay.raft.Server;
import net.purpleclay.raft.util.AbstractServer;
import net.purpleclay.raft.util.MajorityConsensusHandler;

/**
 * Implementation of {@code Server} that is used locally to support the
 * logic of RAFT. This class supports the voting and append/heartbeat RPCs,
 * sending commands to the distributed membership and maintaining basic
 * durable state to survive crashes.
 * <p>
 * The durable state is stored in a local file. The parent directory must be
 * defined via the {@code STATE_DIR_PROPERTY} property. The directory will be
 * created if it doesn't exist (when the parent directory does) when a local
 * server instance is created with no existing state on-disk. Two factory
 * methods, {@code loadInstance} and {@code createInstance} are provided to
 * instantiate a {@code LocalServer}.
 * <p>
 * The interval between hearbeat messages from the leader to all followers is
 * defined by the {@code HEARTBEAT_PROPERTY} property. The value is a time in
 * milliseconds and defaults to 5000ms (5 seconds). The time that a follower
 * waits to hear a heartbeat before it times out and announces candidacy for a
 * new term is defined by the {@code TERM_TIMEOUT_PROPERTY} property. By
 * default this is twice the heartbeat timeout. The term timeout value must be
 * greater than the heartbeat value. All servers in the cluster should use
 * the same values for both properties.
 */
public class LocalServer extends AbstractServer {

	/** Enumeration for a {@code Server}'s current role. */
	enum Role {
		/** Following updates from a known leader. */
		FOLLOWER,
		/** Waiting to hear the result of an election. */
		CANDIDATE,
		/** Leading all other {@code Server}s in the cluster. */
		LEADER
	}

	// the current Role for this server
	private volatile Role currentRole = Role.FOLLOWER;

	/** Property key defining where the durable state is stored. */
	public static final String STATE_DIR_PROPERTY = "state.dir";

	/** Property key defining the heartbeat interval. */
	public static final String HEARTBEAT_PROPERTY = "raft.heartbeat";

	/** Property key definiing the term timeout threshold. */
	public static final String TERM_TIMEOUT_PROPERTY = "raft.termtimeout";

	/** Default heartbeat interval. */
	public static final String DEFAULT_HEARTBEAT = "5000";

	// the configured heartbeat & term timeout
	private final long heartbeatPeriod;
	private final long termTimeout;

	// state flag marking if this server has been started/shut-down
	private volatile boolean active = false;

	// generator used for randominzing back-off on candidacy collisions
	private final Random timeoutGenerator = new Random();

	// the current leader for the cluster, if known
	private Server currentLeader = null;

	// marker for identifying that a leader is not currently known
	private static final long UNKNOWN_LEADER = -1L;

	// durable state
	private final ServerState state;

	// handle to the cluster membership
	private final MembershipHandle membershipHandle;

	// executor used for heartbeat/timeout future events
	private final ScheduledExecutorService executor =
		Executors.newSingleThreadScheduledExecutor();

	// the current future event .. either the server is the leader and this
	// captures the next heartbeat moment, the server is a candidate and this
	// future is the campaign timeout or the server is a follower and this
	// is the term timeout for deciding to announce candidacy
	private Future<?> currentFutureEvent = null;

	// local manager for the distributed log
	private final Log log;

	// handler for consensus on events like voting or command application
	private final ConsensusHandler consensusHandler;

	// a mapping from log index to the local listener (if any) waiting to be
	// notified on append/failure
	private final Map<Long,CommandResultListener> localListeners =
		new ConcurrentHashMap<Long,CommandResultListener>();

	// a local generator to uniquely identify all remote command requests
	private final AtomicLong requestGenerator = new AtomicLong(1);

	// a mapping from remote command request identifiers to the local listener
	// waiting to hear the result of the request
	private final Map<Long,CommandResultListener> remoteWaiters =
		new ConcurrentHashMap<Long,CommandResultListener>();

	/**
	 * Creates an instance of {@code LocalServer} with no previous state.
	 *
	 * @param serverId the unique identifier for this new server
	 * @param log the {@code Log} used by this server
	 * @param handle the {@code MembershipHandle} for the cluster
	 * @param properties the {@code Properties} for configuring this server
	 *
	 * @return a new instance of {@code LocalServer}
	 *
	 * @throws NullPointerException if no state directory is specified
	 * @throws IllegalArgumentException if durable state cannot be initialzed or
	 *                                  if term timeout is greater than heartbeat
	 * @throws IllegalStateException if there is durable state on-disk
	 * @throws NumberFormatException if a heartbeat or term timeout value is
	 *                               specified but is not a number
	 */
	public static LocalServer createInstance(long serverId, Log log,
											 MembershipHandle handle,
											 Properties properties)
	{
		String stateDir = properties.getProperty(STATE_DIR_PROPERTY);
		try {
			return new LocalServer(new ServerState(stateDir, serverId),
								   log, handle, properties);
		} catch (IOException ioe) {
			throw new IllegalArgumentException("Failed to initialize durable state", ioe);
		}
	}

	/**
	 * Creates an instance of {@code LocalServer} based on existing state. The
	 * latest commit index recorded in the state will be applied to the log.
	 *
	 * @param log the {@code Log} used by this server
	 * @param handle the {@code MembershipHandle} for the cluster
	 * @param properties the {@code Properties} for configuring this server
	 *
	 * @return a new instance of {@code LocalServer}
	 *
	 * @throws NullPointerException if no state directory is specified
	 * @throws IllegalArgumentException if the durable state cannot be used or
	 *                                  if term timeout is greater than heartbeat
	 * @throws IllegalStateException if there is no existing durable state
	 * @throws NumberFormatException if a heartbeat or term timeout value is
	 *                               specified but is not a number
	 */
	public static LocalServer loadInstance(Log log, MembershipHandle handle,
										   Properties properties)
	{
		String stateDir = properties.getProperty(STATE_DIR_PROPERTY);
		try {
			return new LocalServer(new ServerState(stateDir), log, handle, properties);
		} catch (IOException ioe) {
			throw new IllegalArgumentException("Failed to setup durable state", ioe);
		}
	}
	
	/** Creates an instance of {@code LocalServer}. */
	private LocalServer(ServerState state, Log log, MembershipHandle membershipHandle,
					   Properties props)
	{
		super(state.getServerId());

		this.state = state;
		this.membershipHandle = membershipHandle;
		this.log = log;

		// TODO: should this always be applied? Should it be a property? Should
		// there be a log/state UID match to be sure this is correct?
		log.applied(state.getCommitIndex());

		// TODO: this should be a plugin option
		this.consensusHandler = new MajorityConsensusHandler(membershipHandle);
		consensusHandler.updateTerm(state.getCurrentTerm());

		this.heartbeatPeriod =
			Long.parseLong(props.getProperty(HEARTBEAT_PROPERTY, DEFAULT_HEARTBEAT));
		String timeoutStr = props.getProperty(TERM_TIMEOUT_PROPERTY);
		this.termTimeout = (timeoutStr == null) ? heartbeatPeriod * 2L :
			Long.parseLong(timeoutStr);

		if (termTimeout < heartbeatPeriod)
			throw new IllegalArgumentException("heartbeat must be smaller than term timeout");
	}

	/* Implement Server */

	@Override public void start() {
		if (active)
			return;
		active = true;

		// TODO: for testing it's convenient to push servers into candidacy
		// which often causes races in corner-cases .. in production, however,
		// should this just always start as a follower and wait to time-out?

		if (membershipHandle.getMembershipCount() < 2)
			convertToCandidate();
		else
			convertToFollower(UNKNOWN_LEADER);
	}

	@Override public void shutdown() {
		if (! active)
			return;
		active = false;

		if (currentFutureEvent != null)
			currentFutureEvent.cancel(false);
		executor.shutdown();

		state.shutdown();
	}

	@Override public void invoke(Message message) {
		synchronized (this) {
			if (message.getTerm() > state.getCurrentTerm()) {
				updateTerm(message.getTerm());
				convertToFollower(message.getSenderId());
			}
		}

		final String requestId = message.getIdentifier();
		Message response = null;

		if (requestId.equals(AppendRequestMsg.IDENTIFIER)) {
			response = requestAppendEntries((AppendRequestMsg) message);
		} else if (requestId.equals(AppendResponseMsg.IDENTIFIER)) {
			respondAppendEntries((AppendResponseMsg) message);
		} else if (requestId.equals(VoteRequestMsg.IDENTIFIER)) {
			response = requestVote((VoteRequestMsg) message);
		} else if (requestId.equals(VoteResponseMsg.IDENTIFIER)) {
			respondVote((VoteResponseMsg) message);
		} else if (requestId.equals(CommandRequestMsg.IDENTIFIER)) {
			response = requestCommand((CommandRequestMsg) message);
		} else if (requestId.equals(CommandResponseMsg.IDENTIFIER)) {
			respondCommand((CommandResponseMsg) message);
		} else {
			throw new IllegalArgumentException("Unknown message: " + requestId);
		}

		if (response != null)
			membershipHandle.findServer(message.getSenderId()).invoke(response);
	}

	@Override public synchronized void send(Command command) {
		send(command, null);
	}

	@Override public synchronized void send(Command command,
											CommandResultListener listener)
	{
		// if we're inactive, or no leader is known, then reject the request
		if ((! active) || (currentLeader == null)) {
			if (listener != null)
				listener.commandFailed();
			return;
		}

		// if we're the leader then we can just track the listener and do the
		// invocation directly .. otherwise, send the message off to the server
		// that we believe to be the leader and wait to hear more

		Message request = null;
		long currentTerm = state.getCurrentTerm();

		if (currentLeader == this) {
			assert currentRole == Role.LEADER : "leadership mis-match";

			if (listener != null)
				localListeners.put(log.getLastIndex() + 1, listener);

			request = new CommandRequestMsg(getId(), currentTerm, command);
		} else {
			assert currentRole == Role.FOLLOWER : "must be following a server";

			if (listener != null) {
				long id = requestGenerator.getAndIncrement();
				remoteWaiters.put(id, listener);
				request = new CommandRequestMsg(getId(), currentTerm, command, id);
			} else {
				request = new CommandRequestMsg(getId(), currentTerm, command);
			}
		}

		currentLeader.invoke(request);
	}

	/* RPC Logic Routines */

	/** Handles an append request. */
	synchronized AppendResponseMsg requestAppendEntries(AppendRequestMsg msg) {
		// TODO: I think we should never get here as LEADER .. if that's
		// right, then what is the right check to do?

		if (msg.getTerm() < state.getCurrentTerm())
			return new AppendResponseMsg(getId(), msg.getTerm());

		// if the previous entry isn't in our log then we're either behind
		// or we've diverged, so figure out where to try re-syncing

		if (! log.hasEntry(msg.getPrevLogIndex(), msg.getPrevLogTerm())) {
			long resyncIndex = AppendResponseMsg.NO_INDEX;

			// if we just don't have enough entries then maybe the log is just
			// lagging, so re-set to the last appended point
			if (log.getLastIndex() < msg.getPrevLogIndex())
				resyncIndex = log.getLastIndex();
			
			// we diverged, so try going back to the last stable index applied
			// which should always be safe in a correct system
			if (state.getCommitIndex() < msg.getPrevLogIndex())
				resyncIndex = state.getCommitIndex();

			// TODO: if the commit index is at or past the request index, but
			// still doesn't match, then this server's log has diverged into
			// an illegal state .. should we shut down at this point?

			return new AppendResponseMsg(getId(), msg.getTerm(), false, resyncIndex);
		}

		convertToFollower(msg.getSenderId());

		// TODO: if this invalidates entries in the log then what happens to
		// registered command listeners? I think we need to purge them and
		// notify each one that the command was rejected, right?
		log.validateEntry(msg.getPrevLogIndex() + 1, msg.getTerm());

		// in the case where a server fell behind it may get multiple catch-up
		// messages that represent the same state, so guard against that here
		// TODO: should this just wind to the last applied first and the start
		// appending from there instead?

		int i = (int) msg.getPrevLogIndex() + 1;
		for (Command entry : msg.getEntries()) {
			if (! log.hasEntry(i++, msg.getTerm()))
				log.append(entry, msg.getTerm());
		}

		applyIndex(msg.getLeaderCommit());

		assert log.getLastIndex() == msg.getPrevLogIndex() + msg.getEntries().length :
		"log length mis-match at " + getId() + "; local index is " + log.getLastIndex();

		return new AppendResponseMsg(getId(), msg.getTerm(), true, log.getLastIndex());
	}

	/** Handles an append response. */
	synchronized void respondAppendEntries(AppendResponseMsg msg) {
		// TODO: should this message simply get ignored? Or is it valid
		// still to handle it if we're the leader?
		if ((msg.getTerm() < state.getCurrentTerm()) || (currentRole != Role.LEADER))
			return;

		if (msg.getResponse()) {
			long appliedIndex =
				consensusHandler.appended(msg.getSenderId(), msg.getIndex(),
										  log.getCommitIndex());

			if (appliedIndex != 0L) {
				applyIndex(appliedIndex);

				// as an optimization send a heartbeat immediately to notify
				// the cluster about the commit
				sendHeartbeat();
			}
		} else {
			// a follower is out of step, so roll-back to the point they
			// indicated (unless they indicated that there's no need to sync)
			long index = msg.getIndex();
			if (index == AppendResponseMsg.NO_INDEX)
				return;

			Message resync =
				new AppendRequestMsg(getId(), state.getCurrentTerm(), index,
									 log.getTerm(index), log.getEntries(index + 1),
									 log.getCommitIndex());
			membershipHandle.findServer(msg.getSenderId()).invoke(resync);
		}
	}

	/** Handles a vote request. */
	synchronized VoteResponseMsg requestVote(VoteRequestMsg msg) {
		if (msg.getTerm() < state.getCurrentTerm())
			return new VoteResponseMsg(getId(), msg.getTerm(), false);

		// if we've already voted then reject the request unless it's from the
		// same server that we voted for before
		long lastVotedId = state.getLastVotedId();
		if ((lastVotedId != ServerState.NO_VOTE) && (lastVotedId != msg.getCandidateId()))
			return new VoteResponseMsg(getId(), msg.getTerm(), false);

		// if they're behind on the log that we've observed don't vote for them
		if ((msg.getLastLogIndex() < log.getLastIndex()) ||
			(msg.getLastLogTerm() < log.getLastTerm()))
			return new VoteResponseMsg(getId(), msg.getTerm(), false);

		convertToFollower(UNKNOWN_LEADER);

		try {
			state.updateLastVotedId(msg.getSenderId());
		} catch (IOException ioe) {
			// TODO: does the server need to halt?
			System.out.println("Failed to update durable state");
		}

		return new VoteResponseMsg(getId(), msg.getTerm(), true);
	}

	/** Handles a vote response. */
	synchronized void respondVote(VoteResponseMsg msg) {
		if (! msg.getResponse())
			return;

		// make sure this is a response for a current, ongoing election
		if ((msg.getTerm() < state.getCurrentTerm()) || (currentRole != Role.CANDIDATE))
			return;

		if (consensusHandler.receivedVote(msg.getSenderId(), msg.getTerm()))
			convertToLeader();
	}

	/** Handle a command append request. */
	synchronized CommandResponseMsg requestCommand(CommandRequestMsg msg) {
		long currentTerm = state.getCurrentTerm();

		// only leaders can append commands to the log
		if (currentRole != Role.LEADER)
			return msg.isResponseRequested() ?
				new CommandResponseMsg(getId(), currentTerm, msg.getRequestId()) : null;

		// get the current last entry, then append the command
		long prevIndex = log.getLastIndex();
		long prevTerm = log.getLastTerm();
		log.append(msg.getCommand(), currentTerm);

		// send the append command to all other servers
		// TODO: this could skip any sub-set that is behind and trying to catch
		// up but in practice that logic seems more fragile than simple broadcast
		Command [] entries = { msg.getCommand() };
		Message request =
			new AppendRequestMsg(getId(), currentTerm, prevIndex, prevTerm,
								 entries, log.getCommitIndex());
		membershipHandle.invokeAll(request);

		// note that there's already one server (the leader) that has appended
		// the command to the log
		long appliedIndex =
			consensusHandler.appended(getId(), prevIndex + 1, log.getCommitIndex());
		if (appliedIndex != 0L)
			applyIndex(appliedIndex);

		// finally, create a response if requested
		return msg.isResponseRequested() ?
			new CommandResponseMsg(getId(), currentTerm, msg.getRequestId(), prevIndex + 1) : null;
	}

	/** Handle a command append response. */
	synchronized void respondCommand(CommandResponseMsg msg) {
		// we should only get a response if there was a listener asking for it
		CommandResultListener listener =
			remoteWaiters.remove(msg.getRequestId());
		assert listener != null : "a command listener was lost";

		if (! msg.commandAccepted()) {
			listener.commandFailed();
			return;
		}

		// the command has been applied, so check if that change has already
		// made it into our local state machine .. if it has, then notify the
		// listener otherwise put the listener into the local map to get
		// notified when the log does catch up

		if (msg.getEntryIndex() <= log.getCommitIndex()) {
			// TODO: is it possible that we rewound and got back to this index
			// with a different command? I don't think so, but this needs tests
			listener.commandApplied();
		} else {
			localListeners.put(msg.getEntryIndex(), listener);
		}
	}

	/* State Transition Routines */

	private synchronized void updateTerm(long term) {
		if (state.getCurrentTerm() == term)
			return;

		try {
			state.updateCurrentTerm(term);
		} catch (IOException ioe) {
			// TODO: does the server need to halt?
			System.out.println("Failed to update durable state");
		}

		consensusHandler.updateTerm(term);
	}

	private synchronized void convertToFollower(long leaderId) {
		currentRole = Role.FOLLOWER;
		currentLeader = (leaderId == UNKNOWN_LEADER) ?
			null : membershipHandle.findServer(leaderId);
		startElectionTimeout(termTimeout);
	}

	private synchronized void convertToCandidate() {
		currentRole = Role.CANDIDATE;

		long newTerm = state.getCurrentTerm() + 1;
		updateTerm(newTerm);

		try {
			state.updateLastVotedId(getId());
		} catch (IOException ioe) {
			// TODO: does the server need to halt?
			System.out.println("Failed to update durable state");
		}

		if (consensusHandler.receivedVote(getId(), newTerm)) {
			convertToLeader();
		} else {
			// schedule a random timeout for 100-300 ms from now
			startElectionTimeout(timeoutGenerator.nextInt(200) + 100L);

			membershipHandle.invokeAll(new VoteRequestMsg(getId(), newTerm, log.getLastIndex(), log.getLastTerm()));
		}
	}

	private synchronized void convertToLeader() {
		currentRole = Role.LEADER;
		currentLeader = this;

		consensusHandler.updateTerm(state.getCurrentTerm());

		startHeartbeat();
	}

	private void applyIndex(long index) {
		state.updateCommitIndex(index);

		long prevIndex = log.getCommitIndex();
		log.applied(index);

		for (long i = prevIndex + 1; i <= index; i++) {
			CommandResultListener listener = localListeners.remove(i);
			if (listener != null)
				listener.commandApplied();
		}
	}

	/* Future Event Utilities */

	private synchronized void startElectionTimeout(long timeout) {
		if (currentFutureEvent != null)
			currentFutureEvent.cancel(false);

		try {
			currentFutureEvent = executor.schedule(new Runnable() {
					public void run() {
						convertToCandidate();
					}
				}, timeout, TimeUnit.MILLISECONDS);
		} catch (RejectedExecutionException ree) {
			assert executor.isShutdown() : "unexpected scheduling failure";
		}
	}

	private synchronized void startHeartbeat() {
		if (currentFutureEvent != null)
			currentFutureEvent.cancel(false);

		try {
			currentFutureEvent = executor.scheduleAtFixedRate(new Runnable() {
					public void run() {
						sendHeartbeat();
					}
				}, 0L, heartbeatPeriod, TimeUnit.MILLISECONDS);
		} catch (RejectedExecutionException ree) {
			assert executor.isShutdown() : "unexpected scheduling failure";
		}
	}

	private synchronized void sendHeartbeat() {
		// when elections happen back-to-back this node may have scheduled a
		// heartbeat as the leader but not cancelled the task in time when
		// another server took over, so check that we're still in the role
		if (currentRole != Role.LEADER)
			return;

		Message request =
			new AppendRequestMsg(getId(), state.getCurrentTerm(), log.getLastIndex(),
								 log.getLastTerm(), log.getCommitIndex());
		membershipHandle.invokeAll(request);
	}

	/* Package-private accessors for testing only. */

	Role getRole() {
		return currentRole;
	}

	long getTerm() {
		return state.getCurrentTerm();
	}

	long getCommitIndex() {
		return log.getCommitIndex();
	}

}
