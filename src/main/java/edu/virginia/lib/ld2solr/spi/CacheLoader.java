package edu.virginia.lib.ld2solr.spi;

import java.util.Set;

import com.hp.hpl.jena.rdf.model.Resource;

/**
 * A {@link CacheLoader} does much what its name suggests: loads a cache with
 * RDF.
 * 
 * @author ajs6f
 * 
 */
public interface CacheLoader<T extends CacheLoader<T, Cache>, Cache> {

	/**
	 * Assign a cache that this {@link CacheLoader} will load.
	 * 
	 * @param c
	 *            the cache to assign
	 * @return this {@link CacheLoader} for further operation.
	 */
	public T cache(Cache c);

	/**
	 * Accepts a {@link Set} of {@link Resource}s and loads them into the
	 * assigned cache.
	 * 
	 * @param resources
	 *            {@link Resource}s to load.
	 * @return those resources that were successfully loaded
	 */
	public Set<Resource> load(final Iterable<Resource> resources);

	/**
	 * Frees any resources associated with this {@link CacheLoader}.
	 * 
	 * @throws InterruptedException
	 */
	public void shutdown() throws InterruptedException;

}
