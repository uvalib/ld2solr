package edu.virginia.lib.ld2solr.spi;

import edu.virginia.lib.ld2solr.spi.Stage.Acceptor;

/**
 * An {@link Acceptor} that requires information from its preceding
 * {@link Stage} in excess of the typical inputs.
 * 
 * @author ajs6f
 * 
 * @param <Accepts>
 * @param <Produces>
 */
public interface FeedforwardAcceptor<Accepts, Produces> extends Acceptor<Accepts, Produces> {

	public Stage<Accepts> previousStage();

}
