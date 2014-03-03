/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.local;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;

import net.purpleclay.raft.KVStateMachine;
import net.purpleclay.raft.Log;
import net.purpleclay.raft.MembershipHandle;
import net.purpleclay.raft.Message;
import net.purpleclay.raft.NonDurableLog;
import net.purpleclay.raft.Server;
import net.purpleclay.raft.StateMachine;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/** Basic tests against a single {@code LocalServer} instance. */
public class LocalServerTest {

	@Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

	@Test
	public void startServer() throws Exception {
		KVStateMachine sm = new KVStateMachine();
		NonDurableLog log = new NonDurableLog(sm);
		LocalServer server = createServer(sm, log, false);
		
		server.start();
		
		Assert.assertEquals("not leader", server.getRole(), LocalServer.Role.LEADER);
	}

	@Test
	public void restartServer() throws Exception {
		KVStateMachine sm = new KVStateMachine();
		NonDurableLog log = new NonDurableLog(sm);
		LocalServer server = createServer(sm, log, false);
		
		server.start();
		server.shutdown();
		
		server = createServer(sm, log, true);
		
		server.start();
		
		Assert.assertEquals("not leader", server.getRole(), LocalServer.Role.LEADER);
	}
	
	private LocalServer createServer(StateMachine sm, Log log, boolean restart) {
		Properties p = new Properties();
		p.setProperty(LocalServer.STATE_DIR_PROPERTY, tmpDir.getRoot().getAbsolutePath());
		
		SingletonMembershipHandle mh = new SingletonMembershipHandle();
		LocalServer server = null;
		if (restart)
			server = LocalServer.loadInstance(log, mh, p);
		else
			server = LocalServer.createInstance(1L, log, mh, p);
		mh.servers.add(server);
		
		return server;
	}
	
	private static class SingletonMembershipHandle implements MembershipHandle {
		final Collection<Server> servers = new HashSet<Server>();
		@Override public int getMembershipCount() { return 1; }
		@Override public void invokeAll(Message message) { }
		@Override public Server findServer(long id) {
			Server server = servers.iterator().next();
			return id == server.getId() ? server : null;
		}
		@Override public Collection<Server> getServers() { return servers; }
	}
}
