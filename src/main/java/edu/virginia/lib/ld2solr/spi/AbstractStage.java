package edu.virginia.lib.ld2solr.spi;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.lang.Integer.MAX_VALUE;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.util.concurrent.ListeningExecutorService;

/**
 * @author ajs6f
 * 
 */
public abstract class AbstractStage<Produces> implements Stage<Produces> {

	public static final Integer DEFAULT_NUM_THREADS = 10;

	protected ListeningExecutorService threadpool = listeningDecorator(newFixedThreadPool(DEFAULT_NUM_THREADS));

	protected Acceptor<Produces, ?> nextStage;

	@Override
	public void andThen(final Acceptor<Produces, ?> a) {
		this.nextStage = a;
	}

	/**
	 * @return a {@link ListeningExecutorService} that should be used for work
	 *         associated with tasks transiting this stage
	 */
	public ListeningExecutorService threadpool() {
		return threadpool;
	}

	@Override
	public void shutdown() throws InterruptedException {
		this.threadpool.shutdown();
		this.threadpool.awaitTermination(MAX_VALUE, SECONDS);
	}
}
