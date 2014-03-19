package net.purpleclay.raft.util;

import java.util.concurrent.TimeoutException;

import net.purpleclay.raft.client.Server;

public class TestUtils {
	public static Server waitForLeader(Server s, long timeout) throws InterruptedException, TimeoutException {
		long startTime = System.currentTimeMillis();
		while (System.currentTimeMillis() < startTime + timeout) {
			Server leader = s.getLeader();
			if (leader != null) {
				return leader;
			}
			Thread.sleep(100);
		}
		
		throw new TimeoutException("Leader was not elected in time");
	}
}
