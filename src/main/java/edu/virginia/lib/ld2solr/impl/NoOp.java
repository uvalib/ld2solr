package edu.virginia.lib.ld2solr.impl;

import edu.virginia.lib.ld2solr.spi.Stage.Acceptor;

public class NoOp<Accepts, Produces> implements Acceptor<Accepts, Produces> {

	public static <A, P> NoOp<A, P> instance() {
		return new NoOp<A, P>();
	}

	@Override
	public <T extends Acceptor<Produces, ?>> T andThen(final T a) {
		return a;
	}

	@Override
	public void next(final Produces task) {
	}

	@Override
	public void shutdown() {
	}

	@Override
	public void accept(final Accepts task) {
	}

}
