package net.purpleclay.raft.client;

import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;
import net.purpleclay.raft.Command;
import net.purpleclay.raft.CommandResultListener;
import net.purpleclay.raft.KVStateMachine;
import net.purpleclay.raft.NonDurableLog;
import net.purpleclay.raft.util.DelegatingStateMachine;
import net.purpleclay.raft.util.DynamicMembershipHandle;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ClientAPITest {

	@Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();
	
	@Test
	public void startServer() throws Exception {
		KVStateMachine kv = new KVStateMachine();
		DynamicMembershipHandle dmh = new DynamicMembershipHandle();
		DelegatingStateMachine dsm = new DelegatingStateMachine();
		dsm.addMachine(dmh, DynamicMembershipHandle.COMMAND_ID);
		dsm.addMachine(kv, KVStateMachine.COMMAND_ID);

		NonDurableLog log = new NonDurableLog(dsm);
		
		Server server = new ServerBuilder().withStateMachine(dsm)
				.withStateDir(tmpDir.getRoot().getAbsolutePath())
				.withLog(log)
				.withMembershipHandle(dmh)
				.init()
				.build();
		
		server.start();
		
	}

	@Test
	public void writeValue() throws Exception {
		KVStateMachine kv = new KVStateMachine();
		DynamicMembershipHandle dmh = new DynamicMembershipHandle();
		DelegatingStateMachine dsm = new DelegatingStateMachine();
		dsm.addMachine(dmh, DynamicMembershipHandle.COMMAND_ID);
		dsm.addMachine(kv, KVStateMachine.COMMAND_ID);

		NonDurableLog log = new NonDurableLog(dsm);
		
		Server server = new ServerBuilder().withStateMachine(dsm)
				.withStateDir(tmpDir.getRoot().getAbsolutePath())
				.withLog(log)
				.withMembershipHandle(dmh)
				.init()
				.build();
		
		server.start();
		Thread.sleep(2000);
		
		final AtomicBoolean success = new AtomicBoolean(false);
		Command command = KVStateMachine.createCommand("key", "value");
		CommandResultListener listener = new CommandResultListener() {
			@Override
			public void commandApplied() {
				success.set(true);
			}

			@Override
			public void commandFailed() {
				Assert.fail("Command Failed");
			}
		};
		
		server.send(command, listener);
		Assert.assertTrue("Write was not successful", success.get());
	}
	
}
