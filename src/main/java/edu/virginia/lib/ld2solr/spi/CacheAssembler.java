package edu.virginia.lib.ld2solr.spi;

import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;

import java.util.Set;

import com.hp.hpl.jena.rdf.model.Resource;

/**
 * A {@link CacheAssembler} does much what its name suggests: loads a cache with
 * RDF.
 * 
 * @author ajs6f
 * 
 */
public interface CacheAssembler<T extends CacheAssembler<T, CacheType>, CacheType> {

	public static final Resource traversableForRecursiveRetrieval = createResource("info:ld2solr/traversableForRecursiveRetrieval");

	/**
	 * Assign a cache that this {@link CacheAssembler} will load.
	 * 
	 * @param c
	 *            the cache to assign
	 * @return this {@link CacheAssembler} for further operation.
	 */
	public T cache(CacheType c);

	/**
	 * @return the cache over which this {@link CacheAssembler} operates
	 */
	public CacheType cache();

	/**
	 * Accepts a {@link Set} of {@link Resource}s and loads them into the
	 * assigned cache.
	 * 
	 * @param resources
	 *            {@link Resource}s to load.
	 * @return those resources that were successfully loaded
	 */
	public void assemble(final Set<Resource> resources);

	/**
	 * Frees any resources associated with this {@link CacheAssembler}.
	 * 
	 * @throws InterruptedException
	 */
	public void shutdown() throws InterruptedException;

	/**
	 * @return URIs of those resources that have successfully been assembled
	 *         into the cache
	 */
	public Set<Resource> successfullyAssembled();

	/**
	 * @return URIs of those resources that this {@link CacheAssembler} has
	 *         attempted to assemble into the cache
	 */
	public Set<Resource> attempted();

	/**
	 * @param t
	 *            the timeout to use while waiting for a parcel of resources to
	 *            fully load into cache, in milliseconds
	 * @return this {@link CacheAssembler} for further operation
	 */
	public T timeout(final long t);

	public static final long DEFAULT_TIMEOUT = 10000;

}
