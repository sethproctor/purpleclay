/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft;

import java.util.ArrayList;
import java.util.List;


/** Non-durable implementation of {@code Log} for testing. */
public class NonDurableLog implements Log {

	private final List<Entry> entries = new ArrayList<Entry>();

	private volatile long commitIndex = 0L;

	private final StateMachine stateMachine;

	public NonDurableLog(StateMachine stateMachine) {
		this.stateMachine = stateMachine;

		entries.add(new Entry(0L, null));
	}

	@Override public long getCommitIndex() {
		return commitIndex;
	}

	@Override public synchronized long getLastIndex() {
		return entries.size() - 1;
	}

	@Override public synchronized long getLastTerm() {
		return entries.get((int) getLastIndex()).term;
	}

	@Override public synchronized boolean hasEntry(long index, long term) {
		if ((index < 0) || (index >= entries.size()))
			return false;
		return entries.get((int) index).term == term;
	}

	@Override public synchronized long getTerm(long index) {
		if ((index < 0) || (index >= entries.size()))
			throw new IllegalArgumentException("Invalid log index: " + index);
		return entries.get((int) index).term;
	}

	@Override public synchronized void validateEntry(long index, long term) {
		if (index >= entries.size())
			return;
		if (entries.get((int) index).term == term)
			return;
		for (int count = entries.size() - (int)index; count > 0; count--)
			entries.remove(index);
	}

	@Override public synchronized void append(Command command, long term) {
		entries.add(new Entry(term, command));
	}

	@Override public synchronized void applied(long appliedIndex) {
		if (appliedIndex <= commitIndex) 
			return;

		long prevCommitIndex = commitIndex;
		commitIndex = Math.min(appliedIndex, getLastIndex());

		for (long i = prevCommitIndex + 1; i <= commitIndex; i++)
			stateMachine.apply(entries.get((int) i).command);
	}

	@Override public synchronized Command [] getEntries(long startingIndex) {
		if ((startingIndex < 1) || (startingIndex >= entries.size()))
			return new Command[0];
		Command [] commandArray = new Command[entries.size() - (int) startingIndex];
		for (int i = 0; i < commandArray.length; i++)
			commandArray[i] = entries.get((int) startingIndex + i).command;
		return commandArray;
	}
	
	private class Entry {
		final long term;
		final Command command;
		Entry(long term, Command command) {
			this.term = term;
			this.command = command;
		}
	}

}
