package edu.virginia.lib.ld2solr.spi;

import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;

import java.util.Set;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * A {@link CacheAssembler} does much what its name suggests: loads a cache with
 * RDF.
 * 
 * @author ajs6f
 * 
 */
public interface CacheAssembler<T extends CacheAssembler<T, Cache>, Cache> {

	public static final Resource traversableForRecursiveRetrieval = createResource("info:ld2solr/traversableForRecursiveRetrieval");

	/**
	 * Assign a cache that this {@link CacheAssembler} will load.
	 * 
	 * @param c
	 *            the cache to assign
	 * @return this {@link CacheAssembler} for further operation.
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
	public Set<Resource> load(final Set<Resource> resources);

	/**
	 * Frees any resources associated with this {@link CacheAssembler}.
	 * 
	 * @throws InterruptedException
	 */
	public void shutdown() throws InterruptedException;

	/**
	 * @param o
	 *            the ontology to use for inference in support of recursive
	 *            retrieval
	 * @return this {@link CacheAssembler} for further operation
	 */
	public T ontology(final OntModel o);

	/**
	 * @return the ontology in use for inference in support of recursive
	 *         retrieval, or null for no recursive retrieval at all.
	 */
	public Model ontology();

}
