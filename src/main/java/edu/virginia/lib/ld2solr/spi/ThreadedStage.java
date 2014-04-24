package edu.virginia.lib.ld2solr.spi;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.lang.Integer.MAX_VALUE;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.util.concurrent.ListeningExecutorService;

/**
 * A {@link Stage} that provides facilities for threaded operation.
 * 
 * @author ajs6f
 * 
 */
public abstract class ThreadedStage<T extends ThreadedStage<T, Produces>, Produces> implements Stage<Produces> {

	public static final Integer DEFAULT_NUM_THREADS = 10;

	protected ListeningExecutorService threadpool = listeningDecorator(newFixedThreadPool(DEFAULT_NUM_THREADS));

	protected Acceptor<Produces, ?> nextStage;

	/*
	 * (non-Javadoc)
	 * 
	 * @see Stage#andThen(Acceptor)
	 */
	@Override
	public <A extends Acceptor<Produces, ?>> A andThen(final A a) {
		this.nextStage = a;
		return a;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see Stage#next(java.lang.Object)
	 */
	@Override
	public void next(final Produces task) {
		nextStage.accept(task);
	}

	/**
	 * @return a {@link ListeningExecutorService} that should be used for work
	 *         associated with tasks transiting this stage
	 */
	protected ListeningExecutorService threadpool() {
		return threadpool;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see Stage#shutdown()
	 */
	@Override
	public void shutdown() throws InterruptedException {
		this.threadpool.shutdown();
		this.threadpool.awaitTermination(MAX_VALUE, SECONDS);
	}

	/**
	 * Alter the number of threads being used in this {@link Stage}.
	 * 
	 * @param numThreads
	 *            the number of threads to use
	 * @return this {@link ThreadedStage} for continued operation
	 * @throws InterruptedException
	 */

	public T threads(final Integer numThreads) throws InterruptedException {
		shutdown();
		threadpool = listeningDecorator(newFixedThreadPool(numThreads));
		@SuppressWarnings("unchecked") final T t = (T) this;
		return t;
	}
}
