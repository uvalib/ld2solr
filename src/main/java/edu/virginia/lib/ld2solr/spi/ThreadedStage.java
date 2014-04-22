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
public abstract class ThreadedStage<T extends ThreadedStage<T, Produces>, Produces> implements Stage<Produces> {

	public static final Integer DEFAULT_NUM_THREADS = 10;

	protected ListeningExecutorService threadpool = listeningDecorator(newFixedThreadPool(DEFAULT_NUM_THREADS));

	protected Acceptor<Produces, ?> nextStage;

	@Override
	public void andThen(final Acceptor<Produces, ?> a) {
		this.nextStage = a;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.virginia.lib.ld2solr.spi.Stage#next(java.lang.Object)
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
	 * @throws InterruptedException
	 */
	public T threads(final Integer numThreads) throws InterruptedException {
		shutdown();
		threadpool = listeningDecorator(newFixedThreadPool(numThreads));
		@SuppressWarnings("unchecked")
		final T t = (T) this;
		return t;
	}
}
