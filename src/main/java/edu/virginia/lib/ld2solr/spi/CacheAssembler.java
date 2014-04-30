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

}
