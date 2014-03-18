package net.purpleclay.raft.client;

import java.util.Properties;

import net.purpleclay.raft.Log;
import net.purpleclay.raft.MembershipHandle;
import net.purpleclay.raft.StateMachine;
import net.purpleclay.raft.local.LocalServer;

public class ServerBuilder {
	
	private boolean init = false;
	private String stateDir = null;
	private MembershipHandle mHandle = null;
	private StateMachine sMachine = null;
	private Log raftLog = null;
	private long heartbeatTimeout = -1L;
	private long termTimeout = -1L;		
	
	public ServerBuilder withLog(Log log) {
		raftLog = log;
		return this;
	}
	public ServerBuilder withStateMachine(StateMachine sm) {
		sMachine = sm;
		return this;
	}
	public ServerBuilder withMembershipHandle(MembershipHandle mh) {
		mHandle = mh;
		return this;
	}
	public ServerBuilder withStateDir(String dir) {
		stateDir = dir;
		return this;
	}
	public ServerBuilder withHeartbeatInterval(long timeout) {
		heartbeatTimeout = timeout;
		return this;
	}
	public ServerBuilder withTermTimeout(long timeout) {
		termTimeout = timeout;	
		return this;
	}
	public ServerBuilder init() {
		init = true;
		return this;
	}
	
	public Server build() {
		if (raftLog == null) {
			throw new IllegalStateException("RAFT Log has not been set.");
		}
		
		if (sMachine == null) {
			throw new IllegalStateException("State Machine has not been set.");
		}
		
		if (mHandle == null) {
			throw new IllegalStateException("Membership Handle has not been set.");
		}
		
		if (stateDir == null) {
			throw new IllegalStateException("State Dir has not been set.");
		}
		
		Properties p = new Properties();
		p.setProperty(LocalServer.STATE_DIR_PROPERTY, stateDir);
		if (heartbeatTimeout > -1L) {
			p.setProperty(LocalServer.HEARTBEAT_PROPERTY, String.valueOf(heartbeatTimeout));
		}
		if (termTimeout > -1L) {
			p.setProperty(LocalServer.TERM_TIMEOUT_PROPERTY, String.valueOf(termTimeout));
		}
		
		LocalServer server = null;
		if (init) {
			server = LocalServer.createInstance(1L, raftLog, mHandle, p);
		} else {
			server = LocalServer.loadInstance(raftLog, mHandle, p);
		}
				
		return server;
		
	}
}
