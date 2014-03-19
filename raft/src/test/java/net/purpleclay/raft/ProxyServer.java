/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.purpleclay.raft.client.Server;
import net.purpleclay.raft.util.AbstractServer;


/**
 * Implementation of {@code Server} for testing that hands-off messages and
 * commands to separate threads (simulating network transfer) and allows for
 * those hand-off points to ignore messages (simulating network failures).
 */
public class ProxyServer extends AbstractServer {

	private final InternalServer server;

	private final boolean allowManagement;

	private volatile boolean active = true;

	private final ExecutorService messageExecutor;
	private final ExecutorService commandExecutor;

	public ProxyServer(InternalServer server, int threadCount) {
		this(server, threadCount, true);
	}
	
	public ProxyServer(InternalServer server, int threadCount, boolean allowManagement) {
		super(server.getId());

		this.server = server;
		this.allowManagement = allowManagement;
		this.messageExecutor = Executors.newFixedThreadPool(threadCount);
		this.commandExecutor = Executors.newFixedThreadPool(threadCount);
	}

	@Override public void start() {
		if (allowManagement)
			server.start();
	}

	@Override public void shutdown() {
		if (allowManagement)
			server.shutdown();
	}

	@Override public void invoke(Message message) {
		if (active)
			messageExecutor.execute(new MessageRunnable(message));
	}

	@Override public void send(Command command) {
		send(command, null);
	}

	@Override public void send(Command command, CommandResultListener listener) {
		if (active)
			commandExecutor.execute(new CommandRunnable(command, listener));
	}
	
	@Override public Server getLeader() {
		return server.getLeader();
	}

	public void disconnect() {
		this.active = false;
	}

	public void reconnect() {
		this.active = true;
	}

	private class MessageRunnable implements Runnable {
		private final Message message;
		MessageRunnable(Message message) {
			this.message = message;
		}
		public void run() {
			server.invoke(message);
		}
	}

	private class CommandRunnable implements Runnable {
		private final Command command;
		private final CommandResultListener listener;
		CommandRunnable(Command command, CommandResultListener listener) {
			this.command = command;
			this.listener = listener;
		}
		public void run() {
			server.send(command, listener);
		}
	}

}
