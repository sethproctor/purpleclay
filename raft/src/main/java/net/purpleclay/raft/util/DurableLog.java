/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import net.purpleclay.raft.Command;
import net.purpleclay.raft.Log;
import net.purpleclay.raft.StateMachine;


/**
 * A basic {@code Log} implementation that writes all commands to a local
 * file. This class makes no attempt at compaction or other optimizations. It
 * is provided as a simple, correct version supporting the RAFT logic.
 * <p>
 * The {@code LOG_DIR_PROPERTY} property must be set to the root directory for
 * the log. The directory will be created if it doesn't exist (when the parent
 * directory does). If there is an existing log in the directory then this will
 * be read and re-played into the state machines up to the last known index
 * applied (committed).
 */
public class DurableLog implements Log {

	// the current version for on-disk state
	private static final long CURRENT_VERSION = 1L;

	// base string for all properties used by this class
	private static final String PROP_BASE = DurableLog.class.getName() + ".";

	/** Property key defining where the log is stored. */
	public static final String LOG_DIR_PROPERTY = PROP_BASE + "logDir";

	/** File name for the command data stored in the log directory. */
	public static final String COMMAND_FILE = "commands";

	// the File used to access the log command data
	private final RandomAccessFile commandFile;

	// a single read-buffer used on re-start or re-wind to access data on disk
	private final byte [] readBuffer = new byte[Short.MAX_VALUE];

	// a single buffer used to write commands to disk
	private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(Short.MAX_VALUE);

	// basic collection that contains all entries
	private final List<LogEntry> entries = new ArrayList<LogEntry>();

	// the state machine that will consume all applied commands
	private final StateMachine stateMachine;

	// the current commit index
	private volatile long commitIndex = 0L;

	/**
	 * Creates an instance of {@code DurableLog} based on the given properties.
	 *
	 * @param properties the {@code Properties} defining this log's configuration
	 * @param stateMachine the {@code StateMachine} that consumes all commands
	 *
	 * @throws IOException if there is any trouble initializing or accessing
	 *                     the on-disk log data
	 */
	public DurableLog(Properties properties, StateMachine stateMachine)
		throws IOException
	{
		this.stateMachine = stateMachine;

		// check that a log directory was specified and is valid

		String logPath = properties.getProperty(LOG_DIR_PROPERTY, null);
		if (logPath == null)
			throw new IllegalArgumentException("missing base directory");

		File logDir = FileUtil.validateDirectory(logPath);

		// either load an existing log or create a new one if no log is found

		File file = new File(logDir, COMMAND_FILE);
		boolean logExists = file.exists();
		this.commandFile = new RandomAccessFile(file, "rw");

		if (logExists) {
			long version = commandFile.readLong();
			if (version != CURRENT_VERSION)
				throw new IOException("invalid log version: " + version);

			LogEntry entry = readEntry();
			while (entry != null) {
				entries.add(entry);
				entry = readEntry();
			}
		} else {
			commandFile.writeLong(CURRENT_VERSION);

			LogEntry entry = new LogEntry(0L, 0L, null);
			writeEntry(entry);
			entries.add(entry);
		}
	}

	/**
	 * TODO: this should probably be part of the interface..
	 */
	public void shutdown() {
		try {
			commandFile.close();
		} catch (IOException ioe) { }
	}

	/* Implement Log */

	@Override public long getCommitIndex() {
		return commitIndex;
	}

	@Override public long getLastIndex() {
		return entries.size() - 1;
	}

	@Override public long getLastTerm() {
		return entries.get((int) getLastIndex()).term;
	}

	@Override public boolean hasEntry(long index, long term) {
		if ((index < 0) || (index >= entries.size()))
			return false;
		return entries.get((int) index).term == term;
	}

	@Override public long getTerm(long index) {
		if ((index < 0) || (index >= entries.size()))
			throw new IllegalArgumentException("Invalid log index: " + index);
		return entries.get((int) index).term;
	}

	@Override public synchronized void validateEntry(long index, long term) {
		if (index >= entries.size())
			return;

		LogEntry entry = entries.get((int) index);
		if (entry.term == term)
			return;

		// TODO: what if the validation fails but the given index is earlier
		// than last commit? I think exposes a bug in the implementation..

		// the log has entries that were eventually rejected, so make sure
		// the on-disk setup ignores those entries and then prune from the
		// in-memory cache

		try {
			pruneFromEntry(entry);
		} catch (IOException ioe) {
			// TODO: this is a fatal error .. how do we signal that?
			System.out.println(ioe.getMessage());
			throw new RuntimeException("Failed to re-wind log", ioe);
		}

		for (int count = entries.size() - (int)index; count > 0; count--)
			entries.remove(index);
	}

	@Override public void append(Command command, long term) {
		// TODO: what should happen if the term is less than the last term?
		if (term < getLastTerm())
			throw new IllegalArgumentException("Invalid term");

		LogEntry entry = new LogEntry(entries.size(), term, command);

		try {
			writeEntry(entry);
		} catch (IOException ioe) {
			// TODO: this is a fatal error .. how do we signal that?
			System.out.println(ioe.getMessage());
			throw new RuntimeException("Failed to write to log", ioe);
		}

		entries.add(entry);
	}

	@Override public void applied(long appliedIndex) {
		if (appliedIndex <= commitIndex) 
			return;

		// TODO: what should happen if appliedIndex is greater than lastIndex?
		// Does this mean that a bug has been exposed, or is it just that
		// consensus was reached before we heard about some command(s)?

		for (long i = commitIndex + 1; i <= appliedIndex; i++)
			stateMachine.apply(entries.get((int) i).command);

		commitIndex = Math.min(appliedIndex, getLastIndex());
	}

	@Override public Command [] getEntries(long startingIndex) {
		if ((startingIndex < 1) || (startingIndex >= entries.size()))
			return new Command[0];

		Command [] commandArray = new Command[entries.size() - (int) startingIndex];
		for (int i = 0; i < commandArray.length; i++)
			commandArray[i] = entries.get((int) startingIndex + i).command;

		return commandArray;
	}

	/** Simple wrapper for entry details. */
	private static class LogEntry {
		volatile long position;
		final long index;
		final long term;
		final Command command;
		LogEntry(long index, long term, Command command) {
			this(0L, index, term, command);
		}
		LogEntry(long position, long index, long term, Command command) {
			this.position = position;
			this.index = index;
			this.term = term;
			this.command = command;
		}
	}

	// TODO: somewhat inefficient way to marshal commands, but just to start..

	private synchronized LogEntry readEntry() throws IOException {
		long filePosition = commandFile.getFilePointer();
		if (filePosition == commandFile.length())
			return null;
		
		long index = commandFile.readLong();
		long term = commandFile.readLong();
		Command command = readCommand();

		return new LogEntry(filePosition, index, term, command);
	}

	private Command readCommand() throws IOException {
		int length = commandFile.readShort();

		if (length == 0)
			return null;

		commandFile.readFully(readBuffer, 0, length);
		ByteArrayInputStream bytesIn = new ByteArrayInputStream(readBuffer, 0, length);
		ObjectInputStream objIn = new ObjectInputStream(bytesIn);
		try {
			return (Command) objIn.readObject();
		} catch (ClassNotFoundException cnfe) {
			throw new IOException("Failed to read command", cnfe);
		} finally {
			objIn.close();
		}
	}

	private synchronized void writeEntry(LogEntry entry) throws IOException {
		writeBuffer.clear();
		writeBuffer.putLong(entry.index).putLong(entry.term);
		writeCommand(entry.command);
		writeBuffer.flip();

		long filePosition = commandFile.getFilePointer();
		commandFile.getChannel().write(writeBuffer);
		commandFile.getChannel().force(false);
		entry.position = filePosition;
	}

	private void writeCommand(Command command) throws IOException {
		if (command == null) {
			writeBuffer.putShort((short) 0);
			return;
		}

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		ObjectOutputStream objOut = new ObjectOutputStream(bytesOut);
		try {
			objOut.writeObject(command);
			byte [] bytes = bytesOut.toByteArray();
			writeBuffer.putShort((short) bytes.length).put(bytes);
		} finally {
			objOut.close();
		}
	}

	private synchronized void pruneFromEntry(LogEntry entry) throws IOException {
		if (entry.position >= commandFile.length())
			return;

		commandFile.seek(entry.position);
	}

}
