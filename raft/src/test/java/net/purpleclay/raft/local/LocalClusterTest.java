/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.local;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import net.purpleclay.raft.ProxyServer;


/** Basic tests for clusters of {@code LocalServer}s. */
public class LocalClusterTest {

	@Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

	@Test
	public void testThreeServers() throws Exception {
		System.out.println("\nSTARTING TEST FOR 3 SERVERS");
		LocalCluster cluster = new LocalCluster(3, tmpDir.getRoot());
		try {
			basicStressTest(cluster);
		} finally {
			cluster.shutdown();
		}
	}

	@Test
	public void testFourServers() throws Exception {
		System.out.println("\nSTARTING TEST FOR 4 SERVERS");
		LocalCluster cluster = new LocalCluster(3, tmpDir.getRoot());
		try {
			basicStressTest(cluster);
		} finally {
			cluster.shutdown();
		}
	}

	@Test
	public void testFiveServers() throws Exception {
		System.out.println("\nSTARTING TEST FOR 5 SERVERS");
		LocalCluster cluster = new LocalCluster(5, tmpDir.getRoot());
		try {
			basicStressTest(cluster);
		} finally {
			cluster.shutdown();
		}
	}

	@Test
	public void testSevenServers() throws Exception {
		System.out.println("\nSTARTING TEST FOR 7 SERVERS");
		LocalCluster cluster = new LocalCluster(7, tmpDir.getRoot());
		try {
			basicStressTest(cluster);
		} finally {
			cluster.shutdown();
		}
	}

	private void basicStressTest(LocalCluster cluster) throws Exception {
		cluster.start();

		// check that initial election works correctly

		Thread.sleep(2000L);

		Assert.assertTrue("term mis-match", cluster.checkTerms());
		Assert.assertNotNull("no leader elected", cluster.getLeader());

		// send three updates and check that all servers end up in the same state

		cluster.sendUpdate("k", "v1");
		cluster.sendUpdate("k", "v2");
		cluster.sendUpdate("k", "v3");

		Thread.sleep(500L);

		Assert.assertTrue("invalid commit index", cluster.checkCommitIndex());
		Assert.assertTrue("state machines are not synchronized", cluster.checkKey("k"));

		// pick a follower to fail, send some updates, and then check that when
		// the follower is restored it catches up correctly

		ProxyServer offlineServer = cluster.getFollower();
		System.out.println("Disconnecting server: " + offlineServer.getId());
		offlineServer.disconnect();

		cluster.sendUpdate("k", "v4");
		cluster.sendUpdate("k", "v5");

		System.out.println("Reconnecting server: " + offlineServer.getId());
		offlineServer.reconnect();

		Thread.sleep(1000L);

		Assert.assertTrue("invalid commit index", cluster.checkCommitIndex());
		Assert.assertTrue("state machines are not synchronized", cluster.checkKey("k"));

		// finally, test a single blocking send at the leader and at a follower

		Assert.assertTrue("failed to get local command notification",
						  cluster.waitOnUpdate("k", "v6", 1000L));
		Thread.sleep(500L);
		Assert.assertTrue("state machines are not synchronized", cluster.checkKey("k"));

		Assert.assertTrue("failed to get remote command notification",
						  cluster.waitOnUpdate("k", "v7", 1000L, cluster.getFollower()));
		Thread.sleep(1000L);
		Assert.assertTrue("state machines are not synchronized", cluster.checkKey("k"));

	}

}
