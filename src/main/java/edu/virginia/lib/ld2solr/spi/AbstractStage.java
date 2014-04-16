/**
 * 
 */
package edu.virginia.lib.ld2solr.spi;

import com.google.common.util.concurrent.ListeningExecutorService;

/**
 * @author ajs6f
 * 
 */
public abstract class AbstractStage<Produces> implements Stage<Produces> {

	protected ListeningExecutorService threadpool;
	protected Acceptor<Produces, ?> nextStage;

	@Override
	public void andThen(final Acceptor<Produces, ?> a) {
		this.nextStage = a;
	}

	@Override
	public ListeningExecutorService threadpool() {
		return threadpool;
	}

}
