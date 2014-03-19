package net.purpleclay.raft.local;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import net.purpleclay.raft.Command;
import net.purpleclay.raft.CommandResultListener;
import net.purpleclay.raft.InternalServer;
import net.purpleclay.raft.KVStateMachine;
import net.purpleclay.raft.MembershipHandle;
import net.purpleclay.raft.Message;
import net.purpleclay.raft.NonDurableLog;
import net.purpleclay.raft.client.Server;
import net.purpleclay.raft.client.ServerBuilder;
import net.purpleclay.raft.local.LocalServer.Role;
import net.purpleclay.raft.util.AbstractServer;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LocalServerRPCTest {
	
	private RPCTestMembershipHandle mh;
	private LocalServer realServer;
	
	@Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();
	
	@Before
	public void setUp() {
		mh = new RPCTestMembershipHandle();
		realServer = buildRealServer(mh);
	}
	
	@After
	public void tearDown() {
		realServer.shutdown();
		realServer = null;
		mh = null;
	}
	
	@Test
	public void testRequestVote() throws Exception {
		RPCTestServer testServer = new RPCTestServer(mh);
		realServer.start();
		
		VoteRequestMsg request = new VoteRequestMsg(testServer.getId(), 1, 1, 1);
		send(realServer, request);
		VoteResponseMsg voteResponse = testServer.getVoteResponseMsg(1, TimeUnit.SECONDS);
		
		Assert.assertEquals(realServer.getId(), voteResponse.getSenderId());
		Assert.assertEquals(1, voteResponse.getTerm());
		Assert.assertEquals(true, voteResponse.getResponse());
	}
	
	@Test
	public void testRespondVote() throws Exception {
		RPCTestServer testServer = new RPCTestServer(mh);
		realServer.start();
		
		VoteRequestMsg request = testServer.getVoteRequestMsg(15, TimeUnit.SECONDS);
		Assert.assertEquals(realServer.getId(), request.getCandidateId());
		Assert.assertEquals(1, request.getTerm());
		Assert.assertEquals(0, request.getLastLogIndex());
		Assert.assertEquals(0, request.getLastLogTerm());

		VoteResponseMsg response = new VoteResponseMsg(testServer.getId(), 1, true);
		send(realServer, response);
		AppendRequestMsg appendReq = testServer.getAppendRequestMsg(15, TimeUnit.SECONDS);
		Assert.assertEquals(1, appendReq.getTerm());
		Assert.assertEquals(0, appendReq.getPrevLogIndex());
		Assert.assertEquals(0, appendReq.getPrevLogTerm());
		Assert.assertEquals(0, appendReq.getEntries().length);
		Assert.assertEquals(0, appendReq.getLeaderCommit());
		
		Assert.assertTrue("Real server did not become leader", realServer.getRole() == Role.LEADER);
		
	}
	
	@Test
	public void testRequestAppend() throws Exception {
		RPCTestServer testServer = new RPCTestServer(mh);
		realServer.start();
		
		VoteRequestMsg request = new VoteRequestMsg(testServer.getId(), 1, 1, 1);
		send(realServer, request);
		VoteResponseMsg voteResponse = testServer.getVoteResponseMsg(1, TimeUnit.SECONDS);
		
		Assert.assertEquals(realServer.getId(), voteResponse.getSenderId());
		Assert.assertEquals(1, voteResponse.getTerm());
		Assert.assertEquals(true, voteResponse.getResponse());
		
		Command[] commands = {KVStateMachine.createCommand("key1", "value1")};
		AppendRequestMsg appendRequest = new AppendRequestMsg(testServer.getId(), 1, 0, 0, commands, 0);
		send(realServer, appendRequest);
		AppendResponseMsg appendResponse = testServer.getAppendResponseMsg(1, TimeUnit.SECONDS);
		Assert.assertEquals(realServer.getId(), appendResponse.getSenderId());
		Assert.assertEquals(1, appendResponse.getTerm());
		Assert.assertEquals(true, appendResponse.getResponse());
		Assert.assertEquals(1, appendResponse.getIndex());
	}
	
	private static void send(InternalServer server, Message msg) {
//		System.out.println("SEND: " + msg);
		server.invoke(msg);
	}
	
	private LocalServer buildRealServer(RPCTestMembershipHandle mh) {
		KVStateMachine kv = new KVStateMachine();
		NonDurableLog log = new NonDurableLog(kv);
		
		LocalServer realServer = (LocalServer) new ServerBuilder().withStateMachine(kv)
				.withStateDir(tmpDir.getRoot().getAbsolutePath())
				.withLog(log)
				.withMembershipHandle(mh)
				.withHeartbeatInterval(1000)
				.init(1L)
				.build();
		
		mh.addServer(realServer);
		
		return realServer;
	}
	
	private class RPCTestMembershipHandle implements MembershipHandle {
		private final Map<Long,InternalServer> servers = new HashMap<Long,InternalServer>();
		
		void addServer(InternalServer s) {
			servers.put(s.getId(), s);
		}
		
		@Override
		public int getMembershipCount() {
			return servers.size();
		}

		@Override
		public void invokeAll(Message message) {
			for (InternalServer server : servers.values()) {
				if (server.getId() != message.getSenderId())
					server.invoke(message);
			}
		}

		@Override
		public InternalServer findServer(long id) {
			return servers.get(id);
		}

		@Override
		public Collection<InternalServer> getServers() {
			return servers.values();
		}
		
	}
	
	private static long getMaxId(RPCTestMembershipHandle mh) {
		long maxId = -1L;
		for (InternalServer s : mh.getServers()) {
			maxId = Math.max(maxId, s.getId());
		}
		return maxId;
	}
	
	private class RPCTestServer extends AbstractServer {

		private final BlockingQueue<Message> msgQueue = new ArrayBlockingQueue<Message>(1);
		RPCTestServer(RPCTestMembershipHandle mh) {
			super(getMaxId(mh) + 1);
			mh.addServer(this);
		}
		
		Message getMsg(long timeout, TimeUnit unit) throws InterruptedException {
			return msgQueue.poll(timeout, unit);
		}
		
		AppendRequestMsg getAppendRequestMsg(long timeout, TimeUnit unit) throws InterruptedException  {
			Message msg = getMsg(timeout, unit);
			Assert.assertTrue("Expected AppendRequestMsg, got " + msg, msg instanceof AppendRequestMsg);
			return (AppendRequestMsg) msg;
		}
		
		AppendResponseMsg getAppendResponseMsg(long timeout, TimeUnit unit) throws InterruptedException  {
			Message msg = getMsg(timeout, unit);
			Assert.assertTrue("Expected AppendResponseMsg, got " + msg, msg instanceof AppendResponseMsg);
			return (AppendResponseMsg) msg;
		}
		
		CommandRequestMsg getCommandRequestMsg(long timeout, TimeUnit unit) throws InterruptedException  {
			Message msg = getMsg(timeout, unit);
			Assert.assertTrue("Expected CommandRequestMsg, got " + msg, msg instanceof CommandRequestMsg);
			return (CommandRequestMsg) msg;
		}
		
		CommandResponseMsg getCommandResponseMsg(long timeout, TimeUnit unit) throws InterruptedException  {
			Message msg = getMsg(timeout, unit);
			Assert.assertTrue("Expected CommandResponseMsg, got " + msg, msg instanceof CommandResponseMsg);
			return (CommandResponseMsg) msg;
		}
		
		VoteRequestMsg getVoteRequestMsg(long timeout, TimeUnit unit) throws InterruptedException  {
			Message msg = getMsg(timeout, unit);
			Assert.assertTrue("Expected VoteRequestMsg, got " + msg, msg instanceof VoteRequestMsg);
			return (VoteRequestMsg) msg;
		}
		
		VoteResponseMsg getVoteResponseMsg(long timeout, TimeUnit unit) throws InterruptedException  {
			Message msg = getMsg(timeout, unit);
			Assert.assertTrue("Expected VoteResponseMsg, got " + msg, msg instanceof VoteResponseMsg);
			return (VoteResponseMsg) msg;
		}
		
		@Override
		public void invoke(Message message) {
//			System.out.println("RECV: " + message);
			msgQueue.add(message);
		}
		
		@Override
		public void start() { }

		@Override
		public void shutdown() { }

		@Override
		public void send(Command command) { }

		@Override
		public void send(Command command, CommandResultListener listener) { }

		@Override
		public Server getLeader() {
			return null;
		}
		
	}
	
}
