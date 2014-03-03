/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.local;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.purpleclay.raft.CommandResultListener;
import net.purpleclay.raft.KVStateMachine;
import net.purpleclay.raft.MembershipHandle;
import net.purpleclay.raft.Message;
import net.purpleclay.raft.NonDurableLog;
import net.purpleclay.raft.ProxyServer;
import net.purpleclay.raft.Server;


/** Testing utility for a fixed collection of {@code LocalServer} instances. */
public class LocalCluster {

	private final Map<Long,LocalServer> localServers =
		new ConcurrentHashMap<Long,LocalServer>();
	private final Map<Long,ProxyServer> proxyServers =
		new ConcurrentHashMap<Long,ProxyServer>();
	private final Map<Long,KVStateMachine> kvMachines =
		new ConcurrentHashMap<Long,KVStateMachine>();

	private final Properties properties = new Properties();

	private final MembershipHandle membershipHandle = new MembershipHandleImpl();

	public LocalCluster(int serverCount, File tmpDir) {
		this(serverCount, 1, tmpDir, 500L, 1000L);
	}

	public LocalCluster(int size, int threads, File tmpDir,
						long heartbeat, long termTimeout)
	{
		properties.setProperty(LocalServer.HEARTBEAT_PROPERTY,
							   String.valueOf(heartbeat));
		properties.setProperty(LocalServer.TERM_TIMEOUT_PROPERTY,
							   String.valueOf(termTimeout));

		for (long i = 1; i <= size; i++) {
			KVStateMachine kv = new KVStateMachine();
			kvMachines.put(i, kv);

			File stateDir = new File(tmpDir, String.valueOf(i));
			stateDir.mkdir();
			properties.setProperty(LocalServer.STATE_DIR_PROPERTY, stateDir.getAbsolutePath());

			LocalServer ls = LocalServer.createInstance(i, new NonDurableLog(kv),
														membershipHandle, properties);
			localServers.put(i, ls);
			proxyServers.put(i, new ProxyServer(ls, threads));
		}
	}

	public void start() {
		for (Server server : proxyServers.values())
			server.start();
	}

	public void shutdown() {
		for (Server server : proxyServers.values())
			server.shutdown();
	}

	public boolean checkTerms() {
		long term = -1L;
		for (LocalServer server : localServers.values()) {
			if (term == -1L) {
				term = server.getTerm();
			} else {
				if (server.getTerm() != term)
					return false;
			}
		}
		return true;
	}

	public boolean checkCommitIndex() {
		long index = -1L;
		for (LocalServer server : localServers.values()) {
			if (index == -1L) {
				index = server.getCommitIndex();
			} else {
				if (server.getCommitIndex() != index)
					return false;
			}
		}
		return true;
	}

	public boolean checkKey(String key) {
		String value = null;
		for (KVStateMachine machine : kvMachines.values()) {
			if (value == null) {
				value = machine.getValue(key);
			} else {
				if (! value.equals(machine.getValue(key)))
					return false;
			}
		}
		return true;
	}

	public ProxyServer getLeader() {
		ProxyServer leader = null;
		for (LocalServer server : localServers.values()) {
			if (server.getRole() == LocalServer.Role.LEADER) {
				if (leader != null)
					throw new IllegalStateException("multiple leaders");
				leader = proxyServers.get(server.getId());
			}
		}
		return leader;
	}

	public ProxyServer getFollower() {
		for (LocalServer server : localServers.values()) {
			if (server.getRole() == LocalServer.Role.FOLLOWER)
				return proxyServers.get(server.getId());
		}
		return null;
	}

	public void sendUpdate(String key, String value) {
		Server leader = getLeader();
		if (leader == null)
			throw new IllegalStateException("no leader is known");
		sendUpdate(key, value, leader);
	}

	public void sendUpdate(String key, String value, Server server) {
		server.send(KVStateMachine.createCommand(key, value));
	}

	public boolean waitOnUpdate(String key, String value, long timeout) {
		Server leader = getLeader();
		if (leader == null)
			throw new IllegalStateException("no leader is known");
		return waitOnUpdate(key, value, timeout, leader);
	}

	public boolean waitOnUpdate(String key, String value, long timeout,
								Server server)
	{
		CountDownLatch latch = new CountDownLatch(1);
		UpdateListener listener = new UpdateListener(latch);
		server.send(KVStateMachine.createCommand(key, value), listener);

		try {
			if (! latch.await(timeout, TimeUnit.MILLISECONDS))
				return false;
		} catch (InterruptedException ie) {
			return false;
		}

		return listener.isSet;
	}

	private class MembershipHandleImpl implements MembershipHandle {
		@Override public int getMembershipCount() {
			return proxyServers.size();
		}
		@Override public void invokeAll(Message message) {
			for (Server server : proxyServers.values()) {
				if (server.getId() != message.getSenderId())
					server.invoke(message);
			}
		}
		@Override public Server findServer(long id) {
			return proxyServers.get(id);
		}
		@Override public Collection<Server> getServers() {
			return new HashSet<Server>(proxyServers.values());
		}
	}

	private class UpdateListener implements CommandResultListener {
		private final CountDownLatch latch;
		volatile boolean isSet = false;
		UpdateListener(CountDownLatch latch) {
			this.latch = latch;
		}
		@Override public void commandApplied() {
			isSet = true;
			latch.countDown();
		}
		@Override public void commandFailed() {
			latch.countDown();
		}
	}

}
