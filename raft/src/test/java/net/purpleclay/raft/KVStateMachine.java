/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/** Testing class that provides a key-value {@code StateMachine}. */
public class KVStateMachine implements StateMachine {

	public static final String COMMAND_ID = KVCommand.class.getName();

	private final Map<String,String> kvMap =
		new ConcurrentHashMap<String,String>();

	@Override public void apply(Command command) {
		if (! (command instanceof KVCommand))
			throw new IllegalArgumentException("Unknown command type");

		KVCommand kvCmd = (KVCommand) command;
		kvMap.put(kvCmd.key, kvCmd.value);
	}

	public String getValue(String key) {
		return kvMap.get(key);
	}

	public static Command createCommand(String key, String value) {
		return new KVCommand(key, value);
	}

	private static class KVCommand implements Command, Serializable {
		private static final long serialVersionUID = -6729316535780966854L;
		final String key;
		final String value;
		KVCommand(String key, String value) {
			this.key = key;
			this.value = value;
		}
		@Override public String getIdentifier() {
			return COMMAND_ID;
		}
	}

}
