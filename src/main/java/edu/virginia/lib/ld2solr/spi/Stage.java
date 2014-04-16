package edu.virginia.lib.ld2solr.spi;

import com.google.common.util.concurrent.ListeningExecutorService;

/**
 * A stage of workflow.
 * 
 * @author ajs6f
 * @param <Produces>
 *            the type of thing produced by this stage
 */
public interface Stage<Produces> {

	/**
	 * Assign the next stage of workflow.
	 * 
	 * @param a
	 */
	public void andThen(Acceptor<Produces, ?> a);

	/**
	 * @return a {@link ListeningExecutorService} that should be used for work
	 *         associated with tasks transiting this stage
	 */
	public ListeningExecutorService threadpool();

	/**
	 * A stage of workflow that, since it is not the first, must accept tasks.
	 * 
	 * @author ajs6f
	 * 
	 * @param <S>
	 *            the type of thing accepted by this stage
	 * @param <T>
	 *            the type of thing produced by this stage
	 */
	public interface Acceptor<Accepts, Produces> extends Stage<Produces> {

		/**
		 * Accept a task.
		 * 
		 * @param task
		 */
		public void accept(Accepts task);

	}

}
