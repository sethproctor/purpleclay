/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.local;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import net.purpleclay.raft.util.FileUtil;


/**
 * Utility class that keeps a durable record of key server states. This
 * implementation uses a single file for the durable record. The file is
 * overwritten with each update and so stays fixed-length.
 * <p>
 * TODO: there should be probably be some UUID in the log that can be matched
 * against this state (and used in online negotiation too) to ensure that
 * re-start is being done against the correct components.
 * <p>
 * TODO: This could be pulled into an interface if it's useful as an
 * extension-point for testing or state-management, but that seems a
 * little excessive at this point in development.
 */
class ServerState {

	// the version of the state file
	private static final long CURRENT_VERSION = 1L;

	// the name of the file where state is stored
	private static final String STATE_FILE_NAME = "server.state";

	// constant for marking that no server has been voted for
	final static long NO_VOTE = -1L;

	// a handle to the file
	private final RandomAccessFile stateFile;

	// a single buffer used to write state to disk
	private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(64);

	// the unique identifier for this server
	private final long serverId;
	
	// the current term, index and last vote cast
	private volatile long currentTerm = 0L;
	private volatile long commitIndex = 0L;
	private volatile long lastVotedId = NO_VOTE;

	/**
	 * Creates an instance of {@code ServerState} backed by data in the
	 * given directory. This constructor requires state on-disk.
	 *
	 * @param statePath the path to the state directory
	 *
	 * @throws IOException if local state is missing or there is any
	 *                     error interacting with the local state
	 */
	ServerState(String statePath) throws IOException {
		this(statePath, -1L);
	}

	/**
	 * Creates an instance of {@code ServerState} backed by data in the
	 * given directory. If there is no state on-disk then a server identifier
	 * must be supplied.
	 *
	 * @param statePath the path to the state directory
	 * @param serverId the unique identifier for this server, or -1 if the
	 *                 identity for the server is unknown
	 * 
	 * @throws IOException if there is any error accessing the directory
	 *                     or working with previous state on-disk
	 * @throws IllegalStateException if the given server identifier doesn't
	 *                               match the id on-disk, or if there is no
	 *                               state and no identifier was provided
	 */
	ServerState(String statePath, long serverId) throws IOException {
		if (statePath == null)
			throw new NullPointerException("State directory cannot be null");

		File stateDir = FileUtil.validateDirectory(statePath);
		File file = new File(stateDir, STATE_FILE_NAME);
		boolean stateExists = file.exists();
		this.stateFile = new RandomAccessFile(file, "rws");

		if (stateExists) {
			long id = readFile();
			if ((serverId != -1L) && (id != serverId))
				throw new IllegalStateException("server identifier mis-match");
			this.serverId = id;
		} else {
			if (serverId == -1L)
				throw new IllegalStateException("no server identifier provided");
			this.serverId = serverId;
		}
	}

	/** Shuts down management of server state. */
	void shutdown() {
		try {
			stateFile.close();
		} catch (IOException ioe) { }
	}

	/**
	 * Returns the server's unique identifier.
	 * 
	 * @return the server's unique identifier
	 */
	long getServerId() {
		return serverId;
	}

	/**
	 * Updates the current term, which also sets the last vote to NO_VOTE.
	 * The changes are persisted to local disk.
	 * 
	 * @param term the current term
	 * 
	 * @throws IOException if there is any problem persisting the change
	 */
	void updateCurrentTerm(long term) throws IOException {
		if (term == currentTerm)
			return;

		writeFile();

		currentTerm = term;
		lastVotedId = NO_VOTE;
	}

	/**
	 * Returns the current term.
	 * 
	 * @return the current term
	 */
	long getCurrentTerm() {
		return currentTerm;
	}

	/**
	 *  Updates the latest commit index. This routine makes a best-effort to
	 *  persist the changes to local-disk but because commit index is not
	 *  required to be durable for correctness this routine will not raise an
	 *  exception if writing to disk fails.
	 * 
	 * @param index the new commit index
	 */
	void updateCommitIndex(long index) {
		if (index == commitIndex)
			return;

		commitIndex = index;

		try {
			writeFile();
		} catch (IOException ioe) {
			// TODO: log this case
			System.out.println("WARNING: failed to write commit index");
		}
	}

	/**
	 * Returns the current commit index.
	 * 
	 * @return the current commit index
	 */
	long getCommitIndex() {
		return commitIndex;
	}

	/**
	 * Updates the last voted server identifier. The changes are persisted
	 * to local disk.
	 * 
	 * @param serverId the identifier for the server last voted for
	 * 
	 * @throws IOException if there is any problem persisting the change
	 */
	void updateLastVotedId(long serverId) throws IOException {
		if (serverId == lastVotedId)
			return;

		writeFile();

		lastVotedId = serverId;
	}

	/**
	 * Returns the last voted for server identifier
	 * 
	 * @return the last voted for server identifier
	 */
	long getLastVotedId() {
		return lastVotedId;
	}

	/** Reads the complete file into local state. */
	private synchronized long readFile() throws IOException {
		long version = stateFile.readLong();
		if (version != CURRENT_VERSION)
			throw new IOException("Invalid file");

		long id = stateFile.readLong();
		
		currentTerm = stateFile.readLong();
		commitIndex = stateFile.readLong();
		lastVotedId = stateFile.readLong();

		return id;
	}

	/** Writes the complete state into the local file. */
	private synchronized void writeFile() throws IOException {
		writeBuffer.clear();
		writeBuffer.putLong(CURRENT_VERSION).putLong(serverId).
			putLong(currentTerm).putLong(commitIndex).putLong(lastVotedId);
		writeBuffer.flip();

		stateFile.getChannel().write(writeBuffer);
		stateFile.getChannel().force(false);
	}

}
