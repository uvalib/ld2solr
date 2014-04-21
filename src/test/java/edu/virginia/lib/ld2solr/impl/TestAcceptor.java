package edu.virginia.lib.ld2solr.impl;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;

import edu.virginia.lib.ld2solr.api.OutputRecord;
import edu.virginia.lib.ld2solr.spi.RecordSink.RecordPersister;
import edu.virginia.lib.ld2solr.spi.Stage.Acceptor;

public class TestAcceptor<Accepts, Produces> implements Acceptor<Accepts, Produces> {

	private final Set<Accepts> accepted = new HashSet<>();

	private static final Logger log = getLogger(TestAcceptor.class);

	@Override
	public void andThen(final Acceptor<Produces, ?> a) {
		// NOOP
	}

	@Override
	public void next(final Produces task) {
		// // NO-OP
	}

	@Override
	public void accept(final Accepts task) {
		log.debug("Accepted: {}", task);
		accepted.add(task);
		synchronized (this) {
			notifyAll();
		}
	}

	public Set<Accepts> accepted() {
		return accepted;
	}

	@Override
	public void shutdown() {
		// NO-OP
	}

	public static class TestSink extends TestAcceptor<OutputRecord, OutputRecord> implements RecordPersister {

		@Override
		public TestSink location(final String location) {
			return this;
		}

		@Override
		public String location() {
			return "/dev/null";
		}
	}
}
