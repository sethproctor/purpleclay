package net.purpleclay.raft.local;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ConcurrentModificationTest {
	
	@Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

	@Test
	public void testThreeServers() throws Exception {
		final LocalCluster cluster = new LocalCluster(3, tmpDir.getRoot());
		try {
			concurrentWriteTest(cluster);
		} finally {
			cluster.shutdown();
		}
	}
	
	@Test
	public void testSevenServers() throws Exception {
		final LocalCluster cluster = new LocalCluster(7, tmpDir.getRoot());
		try {
			concurrentWriteTest(cluster);
		} finally {
			cluster.shutdown();
		}
	}

	private void concurrentWriteTest(final LocalCluster cluster) throws Exception {
		cluster.start();

		// check that initial election works correctly

		Thread.sleep(2000L);

		Assert.assertTrue("term mis-match", cluster.checkTerms());
		Assert.assertNotNull("no leader elected", cluster.getLeader());
		
		final CyclicBarrier gate = new CyclicBarrier(5);
		Runnable setV1 = new Runnable() {
			public void run() {
				try {
					gate.await();
				} catch (InterruptedException e) {

				} catch (BrokenBarrierException e) {
					
				}
				cluster.sendUpdate("k", "v1");
			}
		};
		
		Runnable setV2 = new Runnable() {
			public void run() {
				try {
					gate.await();
				} catch (InterruptedException e) {

				} catch (BrokenBarrierException e) {
					
				}
				cluster.sendUpdate("k", "v2");
			}
		};
		
		Runnable setV3 = new Runnable() {
			public void run() {
				try {
					gate.await();
				} catch (InterruptedException e) {

				} catch (BrokenBarrierException e) {
					
				}
				cluster.sendUpdate("k", "v3");
			}
		};
		
		ExecutorService pool = Executors.newFixedThreadPool(30);
		for (int i = 0; i < 100; i++) {
			pool.execute(setV1);
			pool.execute(setV2);
			pool.execute(setV3);
		}
		pool.shutdown();
		pool.awaitTermination(10, TimeUnit.SECONDS);
		Thread.sleep(500);
		
		Assert.assertTrue("invalid commit index", cluster.checkCommitIndex());
		Assert.assertTrue("state machines are not synchronized", cluster.checkKey("k"));
		
	}


	
	
}

