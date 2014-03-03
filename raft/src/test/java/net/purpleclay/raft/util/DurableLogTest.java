/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.util;

import java.io.IOException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import net.purpleclay.raft.KVStateMachine;
import net.purpleclay.raft.Log;
import net.purpleclay.raft.StateMachine;


/** Basic tests for creating & re-starting a {@code DurableLog}. */
public class DurableLogTest {

	@Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

	@Test
	public void testStart() throws Exception {
		Properties p = new Properties();
		p.setProperty(DurableLog.LOG_DIR_PROPERTY, tmpDir.getRoot().getAbsolutePath());

		KVStateMachine kv = new KVStateMachine();
		Log log = createLog(p, kv);

		Assert.assertEquals("invalid commit index", 0L, log.getCommitIndex());
		Assert.assertEquals("invalid last index", 0L, log.getLastIndex());
		Assert.assertEquals("invalid last term", 0L, log.getLastTerm());

		log.append(KVStateMachine.createCommand("k", "v1"), 1L);
		log.append(KVStateMachine.createCommand("k", "v2"), 1L);
		log.append(KVStateMachine.createCommand("k", "v3"), 2L);

		Assert.assertEquals("invalid term", log.getTerm(2L), 1L);
		Assert.assertEquals("invalid commit index", 0L, log.getCommitIndex());
		Assert.assertEquals("invalid last index", 3L, log.getLastIndex());
		Assert.assertEquals("invalid last term", 2L, log.getLastTerm());

		Assert.assertNull("value was applied early", kv.getValue("k"));

		log.applied(2);
		Assert.assertEquals("invalid state entry", kv.getValue("k"), "v2");

		log.applied(3);
		Assert.assertEquals("invalid state entry", kv.getValue("k"), "v3");

		Assert.assertEquals("invalid commit index", 3L, log.getCommitIndex());

		// TODO: if this gets hoisted into the interface remove the cast
		((DurableLog) log).shutdown();

		// re-start against the current on-disk state

		kv = new KVStateMachine();
		log = createLog(p, kv);

		log.validateEntry(1L, 1L);
		log.validateEntry(4L, 3L);

		Assert.assertTrue("missing entry", log.hasEntry(1L, 1L));
		Assert.assertTrue("missing entry", log.hasEntry(2L, 1L));
		Assert.assertTrue("missing entry", log.hasEntry(3L, 2L));
		Assert.assertFalse("unexpected entry", log.hasEntry(4L, 2L));

		Assert.assertEquals("invalid last index", 3L, log.getLastIndex());
		Assert.assertEquals("invalid last term", 2L, log.getLastTerm());

		Assert.assertEquals("invalid commit index", 0L, log.getCommitIndex());
		log.applied(3L);
		Assert.assertEquals("invalid commit index", 3L, log.getCommitIndex());
	}

	/** Abstraction so that this test suite could be re-used for other logs. */
	private Log createLog(Properties p, StateMachine sm) throws IOException {
		return new DurableLog(p, sm);
	}

}
