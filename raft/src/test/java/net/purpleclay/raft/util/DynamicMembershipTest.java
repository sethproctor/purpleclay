/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.util;

import java.io.File;
import java.util.Properties;

import net.purpleclay.raft.KVStateMachine;
import net.purpleclay.raft.NonDurableLog;
import net.purpleclay.raft.ProxyServer;
import net.purpleclay.raft.local.LocalServer;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/** Tests for {@code DynamicMembershipHandle}. */
public class DynamicMembershipTest {

	@Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

	@Test
	public void bootstrapFirstServerTest() throws Exception {
		DynamicMembershipHandle dmh = new DynamicMembershipHandle();
		LocalServer server = addServer(1L, dmh);
		server.start();
		server.send(DynamicMembershipHandle.createAddCommand(server.getId()));

		Assert.assertTrue("wrong server type", dmh.findServer(1L) instanceof LocalServer);
	}

	@Test
	public void growFromOneToThreeTest() throws Exception {
		DynamicMembershipHandle dmh = new DynamicMembershipHandle();
		LocalServer server = addServer(1L, dmh);
		server.start();
		server.send(DynamicMembershipHandle.createAddCommand(server.getId()));

		DynamicMembershipHandle dmh2 = new DynamicMembershipHandle();
		LocalServer server2 = addServer(2L, dmh2);
		server.send(DynamicMembershipHandle.createAddCommand(server2.getId()));

		dmh.registerServer(new ProxyServer(server2, 1, false));
		dmh2.registerServer(new ProxyServer(server, 1, false));

		Thread.sleep(1000L);

		Assert.assertTrue("wrong server type", dmh.findServer(1L) instanceof LocalServer);
		Assert.assertTrue("wrong server type", dmh2.findServer(2L) instanceof LocalServer);
		
		DynamicMembershipHandle dmh3 = new DynamicMembershipHandle();
		LocalServer server3 = addServer(3L, dmh3);

		dmh.registerServer(new ProxyServer(server3, 1, false));
		dmh2.registerServer(new ProxyServer(server3, 1, false));
		dmh3.registerServer(new ProxyServer(server, 1, false));
		dmh3.registerServer(new ProxyServer(server2, 1, false));

		// test that either ordering works
		server.send(DynamicMembershipHandle.createAddCommand(server3.getId()));
		
		Thread.sleep(1000L);

		Assert.assertTrue("wrong server type", dmh.findServer(1L) instanceof LocalServer);
		Assert.assertTrue("wrong server type", dmh2.findServer(2L) instanceof LocalServer);
		Assert.assertTrue("wrong server type", dmh3.findServer(3L) instanceof LocalServer);
	}

	private LocalServer addServer(long id, DynamicMembershipHandle dmh) {
		KVStateMachine kv = new KVStateMachine();
		DelegatingStateMachine dsm = new DelegatingStateMachine();
		dsm.addMachine(dmh, DynamicMembershipHandle.COMMAND_ID);
		dsm.addMachine(kv, KVStateMachine.COMMAND_ID);

		NonDurableLog log = new NonDurableLog(dsm);
		
		File dir = new File(tmpDir.getRoot(), String.valueOf(id));
		dir.mkdir();
		Properties p = new Properties();
		p.setProperty("state.dir", dir.getAbsolutePath());
		p.setProperty(LocalServer.HEARTBEAT_PROPERTY, "500");
		p.setProperty(LocalServer.TERM_TIMEOUT_PROPERTY, "1000");

		LocalServer server = LocalServer.createInstance(id, log, dmh, p);
		dmh.registerServer(server);
		
		return server;
	}
}
