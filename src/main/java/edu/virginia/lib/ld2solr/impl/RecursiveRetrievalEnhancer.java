package edu.virginia.lib.ld2solr.impl;

import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.collect.Sets.difference;
import static com.hp.hpl.jena.ontology.OntModelSpec.OWL_MEM_RDFS_INF;
import static com.hp.hpl.jena.query.ReadWrite.WRITE;
import static com.hp.hpl.jena.rdf.model.ModelFactory.createOntologyModel;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Set;

import org.slf4j.Logger;

import com.google.common.base.Function;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.spi.CacheAssembler;
import edu.virginia.lib.ld2solr.spi.CacheEnhancer;

public class RecursiveRetrievalEnhancer implements CacheEnhancer<RecursiveRetrievalEnhancer, Dataset> {

	private CacheAssembler<?, Dataset> cacheAssembler;

	private OntModel ontology;

	private static final Logger log = getLogger(RecursiveRetrievalEnhancer.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see CacheEnhancer#enhance(java.lang.Object)
	 */
	@Override
	public void enhance() {

		final Dataset cache = cacheAssembler.cache();
		// we act inside one large transaction because we may need to recurse
		// and TDB does not support nested transactions
		final Boolean wasInTransaction = cache.isInTransaction();
		if (!wasInTransaction) {
			cache.begin(WRITE);
		}
		try {
			// we use an ontology-enabled model using OWL form and RDFS
			// entailment (for efficiency)
			log.trace("Using retrieval ontology:\n{}", prettyPrintModel(ontology));
			final OntModel model = createOntologyModel(OWL_MEM_RDFS_INF, cache.getDefaultModel().union(ontology));
			recursiveEnhance(model);
			if (!wasInTransaction) {
				cache.commit();
			}
		} finally {
			if (!wasInTransaction) {
				cache.end();
			}
		}
	}

	private void recursiveEnhance(final Model model) {
		final Set<Resource> urisToCache = rdfObjectsToRetrieve(model);
		if (cacheAssembler.attempted().containsAll(urisToCache)) {
			log.trace("We have exhausted the recursively retrievable URIs.");
			return;
		}
		log.debug("Retrieving new URIs to cache: {}.", urisToCache);
		cacheAssembler.assemble(urisToCache);
		// now there may be more traversable links in the cache
		final Set<Resource> plausiblyRetrievableRdfObjects = rdfObjectsToRetrieve(model);
		final Set<Resource> unretrievedButPlausiblyRetrievableURIs = difference(plausiblyRetrievableRdfObjects,
				cacheAssembler.attempted());
		if (!unretrievedButPlausiblyRetrievableURIs.isEmpty()) {
			log.trace("Recursing for unretrieved but plausibly retrievable URIs: {}",
					unretrievedButPlausiblyRetrievableURIs);
			recursiveEnhance(model);
		}
	}

	/**
	 * Determines what URIs in a {@link Model} we may wish to dereference.
	 * 
	 * @param m
	 *            the {@link Model} we examine
	 * @return {@link Resource}s that perhaps should be recursively dereferenced
	 */
	private Set<Resource> rdfObjectsToRetrieve(final Model m) {
		final Set<Resource> uris = copyOf(transform(m.query(new StatementsForRecursiveRetrieval(m)).listObjects(), cast));
		log.trace("Discovered potentially deferenceable URIs: {}", uris);
		return uris;
	}

	private static Function<RDFNode, Resource> cast = new Function<RDFNode, Resource>() {

		@Override
		public Resource apply(final RDFNode n) {
			return n.asResource();
		}
	};

	/*
	 * (non-Javadoc)
	 * 
	 * @see CacheEnhancer#cacheAssembler(CacheAssembler)
	 */
	@Override
	public RecursiveRetrievalEnhancer cacheAssembler(final CacheAssembler<?, Dataset> c) {
		this.cacheAssembler = c;
		return this;
	}

	/**
	 * @param o
	 *            the ontology to use for recursive retrieval
	 * @return this {@link RecursiveRetrievalEnhancer} for further operation
	 */
	public RecursiveRetrievalEnhancer ontology(final OntModel o) {
		this.ontology = o;
		return this;
	}

	private static String prettyPrintModel(final Model m) {
		try (StringWriter w = new StringWriter()) {
			m.write(w);
			return w.toString();
		} catch (final IOException e) {
			// Should never be reached.
			return null;
		}
	}

	@Override
	public void shutdown() {
		// NO OP
	}
}
