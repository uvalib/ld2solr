package edu.virginia.lib.ld2solr.impl;

import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.Sets.difference;
import static com.hp.hpl.jena.query.ReadWrite.READ;

import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Function;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Selector;
import com.hp.hpl.jena.rdf.model.Statement;

import edu.virginia.lib.ld2solr.spi.CacheLoader;
import edu.virginia.lib.ld2solr.spi.ThreadedStage;

public class RecursiveDatasetCacheLoader extends ThreadedStage<RecursiveDatasetCacheLoader, Dataset> implements
		CacheLoader<RecursiveDatasetCacheLoader, Dataset> {

	private Dataset dataset;

	private DatasetCacheLoader loader;

	Set<Resource> successfullyRetrieved = new HashSet<>();

	/*
	 * (non-Javadoc)
	 * 
	 * @see CacheLoader#cache(java.lang.Object)
	 */
	@Override
	public RecursiveDatasetCacheLoader cache(final Dataset d) {
		this.dataset = d;
		this.loader.cache(d);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see CacheLoader#load(java.util.Set)
	 */
	@Override
	public Set<Resource> load(final Iterable<Resource> resources) {
		dataset.begin(READ);
		final Model model = dataset.getDefaultModel();
		dataset.end();

		// determine the next set of resources that may warrant retrieval
		Set<RDFNode> urisToBeRetrieved = copyOf(model.query(new Selector() {

			@Override
			public boolean test(final Statement s) {
				if (s.getObject().isURIResource()) {
					return true;
				}
				return false;
			}

			@Override
			public boolean isSimple() {
				return false;
			}

			@Override
			public Resource getSubject() {
				return null;
			}

			@Override
			public Property getPredicate() {
				return null;
			}

			@Override
			public RDFNode getObject() {
				return null;
			}
		}).listObjects());

		// we avoid re-retrieving resources we have already retrieved
		urisToBeRetrieved = difference(urisToBeRetrieved, successfullyRetrieved);

		// load the next depth of resources in the graph
		successfullyRetrieved.addAll(loader.load(transform(urisToBeRetrieved, cast)));

		// recurse further into the Linked Data graph
		if (successfullyRetrieved.size() > 0) {
			// we avoid re-retrieving resources we have already retrieved
			urisToBeRetrieved = difference(urisToBeRetrieved, successfullyRetrieved);

			// load the next depth of resources in the graph
			successfullyRetrieved.addAll(loader.load(transform(urisToBeRetrieved, cast)));

			load(successfullyRetrieved);
		}

		return successfullyRetrieved;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see ThreadedStage#threads(java.lang.Integer)
	 */
	@Override
	public RecursiveDatasetCacheLoader threads(final Integer numThreads) throws InterruptedException {
		this.loader.threads(numThreads);
		return this;
	}

	private static Function<RDFNode, Resource> cast = new Function<RDFNode, Resource>() {

		@Override
		public Resource apply(final RDFNode n) {
			return (Resource) n;
		}
	};

}
