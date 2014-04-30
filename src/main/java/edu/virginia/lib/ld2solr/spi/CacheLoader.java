package edu.virginia.lib.ld2solr.spi;

import java.util.Set;

import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.IdentifiedModel;
import edu.virginia.lib.ld2solr.spi.Stage.Acceptor;

public interface CacheLoader<T extends CacheLoader<T, CacheType>, CacheType> extends Acceptor<IdentifiedModel, IdentifiedModel> {

	/**
	 * Assign a cache that this {@link CacheLoader} will load.
	 * 
	 * @param c
	 *            the cache to assign
	 * @return this {@link CacheLoader} for further operation.
	 */
	public T cache(CacheType c);

	/**
	 * @return URIs of those resources that have successfully been loaded
	 */
	public Set<Resource> successfullyLoaded();

}
