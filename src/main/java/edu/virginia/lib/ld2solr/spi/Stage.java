package edu.virginia.lib.ld2solr.spi;

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
	 *            the next {@link Stage} of workflow
	 * @return the assigned next stage of workflow for chaining
	 */
	public <T extends Acceptor<Produces, ?>> T andThen(T a);

	/**
	 * Hand a task over to the next stage of workflow.
	 * 
	 * @param task
	 *            the task to be performed by the next {@link Stage}
	 */
	public void next(Produces task);

	/**
	 * Frees any resources associated to this {@link Stage}.
	 * 
	 * @throws InterruptedException
	 */
	public void shutdown() throws InterruptedException;

	/**
	 * A stage of workflow that is not the first and therefore accepts tasks.
	 * 
	 * @author ajs6f
	 * @param <Accepts>
	 *            the type of thing accepted by this stage
	 * @param <Produces>
	 *            the type of thing produced by this stage
	 */
	public interface Acceptor<Accepts, Produces> extends Stage<Produces> {

		/**
		 * Accept a task or quantum of work.
		 * 
		 * @param task
		 *            the task to perform
		 */
		public void accept(Accepts task);
	}

}
