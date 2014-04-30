package edu.virginia.lib.ld2solr.spi;

import java.util.Set;

import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.IdentifiedModel;
import edu.virginia.lib.ld2solr.spi.Stage.Acceptor;

/**
 * A workflow {@link Acceptor} responsible for retrieving the contents of
 * proferred URIs.
 * 
 * @author ajs6f
 * 
 */
public interface CacheRetriever extends Acceptor<Resource, IdentifiedModel> {

	public Set<Resource> successfullyRetrieved();

}
