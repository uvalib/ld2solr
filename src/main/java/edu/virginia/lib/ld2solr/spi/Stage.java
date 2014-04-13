package edu.virginia.lib.ld2solr.spi;

/**
 * @author ajs6f
 * @param <S>
 *            the type of thing produced by this stage
 */
public interface Stage<Produces> {

	public void andThen(Acceptor<Produces, ?> a);

	/**
	 * @author ajs6f
	 * 
	 * @param <S>
	 *            the type of thing accepted by this stage
	 * @param <T>
	 *            the type of thing produced by this stage
	 */
	public interface Acceptor<Accepts, Produces> extends Stage<Produces> {

		public void accept(Accepts task);

	}

}
