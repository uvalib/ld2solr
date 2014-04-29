package edu.virginia.lib.ld2solr.impl;

import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.union;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.JdkFutureAdapters.listenInPoolThread;
import static com.hp.hpl.jena.query.ReadWrite.WRITE;
import static com.hp.hpl.jena.shared.Lock.READ;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Selector;
import com.hp.hpl.jena.rdf.model.Statement;

import edu.virginia.lib.ld2solr.spi.CacheLoader;
import edu.virginia.lib.ld2solr.spi.ThreadedStage;

/**
 * A {@link DatasetCacheLoader} loads Linked Data into a Jena {@link Dataset} .
 * 
 * @author ajs6f
 * 
 */
public class DatasetCacheLoader extends ThreadedStage<DatasetCacheLoader, Void> implements
		CacheLoader<DatasetCacheLoader, Dataset> {

	private static final long TIMEOUT = 10000;

	private static final long TIMESTEP = 1000;

	/**
	 * TODO develop a more elegant idea of limited recursion
	 */
	private static final int RECURSION_LIMIT = 2;

	private Integer recursionLevel = 1;

	private CompletionService<Model> internalQueue;

	private Dataset dataset;

	private final Set<Resource> successfullyLoadedResources = new HashSet<>();

	private final Set<Resource> unsuccessfullyLoadedResources = new HashSet<>();

	/**
	 * We use attemptedResources to determine whether or not to retrieve a
	 * resource, specifically, to avoid retrieving resources more than once. We
	 * use fullyAttemptedResources to determine when a cycle of recursive
	 * retrieval is complete.
	 */
	private final Set<Resource> attemptedResources = new HashSet<>();
	private final Set<Resource> fullyAttemptedResources = union(unsuccessfullyLoadedResources,
			successfullyLoadedResources);

	private String accepts = null;

	private static final Logger log = getLogger(DatasetCacheLoader.class);

	public DatasetCacheLoader() {
		this.internalQueue = new ExecutorCompletionService<Model>(this.threadpool);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see CacheLoader#load(java.util.Set)
	 */
	@Override
	public Set<Resource> load(final Set<Resource> uris) {
		// we make a defensive copy of our resource checklist for thread-safety
		final Set<Resource> resources = copyOf(uris);
		log.debug("Attempting to load: {}", resources);
		log.trace("Operating at recursion level: {}", ++recursionLevel);

		for (final Resource uri : resources) {

			log.info("Queueing retrieval task for URI: {}...", uri);
			attemptedResources.add(uri);

			final Future<Model> loadFuture = internalQueue.submit(new JenaModelTriplesRetriever(accepts).apply(uri));
			final ListenableFuture<Model> loadTask = listenInPoolThread(loadFuture);
			addCallback(loadTask, new FutureCallback<Model>() {

				@Override
				public void onSuccess(final Model result) {
					log.info("Retrieved URI: {} and will add its contents to cache.", uri);
					successfullyLoadedResources.add(uri);
				}

				@Override
				public void onFailure(final Throwable t) {
					log.error("Failed to retrieve: {}!", uri);
					log.error("Exception: ", t);
					unsuccessfullyLoadedResources.add(uri);
				}
			});
		}

		log.debug("Finished queuing retrieval tasks for URIs:\n{}", resources);
		log.trace("{} cache loading tasks left to perform.", resources.size());
		// the only purpose of the following loop is to ensure that we execute
		// as many
		// tasks to add triples to the cache as we have executed tasks to
		// retrieve triples
		for (int tasks = 1; tasks <= resources.size(); tasks++) {
			log.trace("Loading resource {} of {}...", tasks, resources.size());
			try {
				final Model m = internalQueue.take().get();
				if (!m.isEmpty()) {
					m.enterCriticalSection(READ);
					log.debug("Adding {} triples to cache...", m.size());
					log.trace("Adding triples: {}", m);
					try {
						dataset.begin(WRITE);
						try {
							dataset.getDefaultModel().add(m);
							dataset.commit();
						} catch (final Exception e) {
							log.error("Error adding triples to cache!");
							log.error("Triples: ", m);
							log.error("Exception: ", e);
						} finally {
							dataset.end();
						}
					} finally {
						m.leaveCriticalSection();
						m.close();
					}
				}
			} catch (InterruptedException | ExecutionException e) {
				log.error("Error assembling triples to add to cache!");
				log.error("Exception: ", e);
			}
		}
		log.info("Finished loading one span of resources into the cache with URIs:\n{}", resources);

		dataset.begin(ReadWrite.READ);
		final Model model = dataset.getDefaultModel();
		dataset.end();
		model.enterCriticalSection(READ);
		final Set<Resource> objectsInDataset = copyOf(transform(model.query(statementsWithUriObjects).listObjects(),
				cast));
		// we check the resources we might potentially want to retrieve against
		// those we have already tried to retrieve, to avoid re-retrieving
		// resources
		final Set<Resource> resourcesNowToBeRetrieved = difference(objectsInDataset, attemptedResources);
		model.leaveCriticalSection();

		// possibly recurse to next depth of Linked Data graph
		if (!resourcesNowToBeRetrieved.isEmpty() && recursionLevel <= RECURSION_LIMIT) {
			log.debug("Now attempting to recursively retrieve: {}", resourcesNowToBeRetrieved);
			load(resourcesNowToBeRetrieved);
		}
		wait(resources);
		return successfullyLoadedResources;
	}

	/**
	 * wait for certain resources to be resolved
	 * 
	 * @param uris
	 *            the {@link Resource}s on which to wait
	 */
	private void wait(final Set<Resource> uris) {
		final long startTime = currentTimeMillis();
		while (!fullyAttemptedResources.containsAll(uris) && currentTimeMillis() < (startTime + TIMEOUT)) {
			try {
				log.trace("Waiting for {} to be fully loaded into {}...", uris, fullyAttemptedResources);
				sleep(TIMESTEP);
			} catch (final InterruptedException e) {
				break;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see ThreadedStage#threads(java.lang.Integer)
	 */
	@Override
	public DatasetCacheLoader threads(final Integer numThreads) throws InterruptedException {
		super.threads(numThreads);
		this.internalQueue = new ExecutorCompletionService<Model>(this.threadpool);
		return this;
	}

	/**
	 * @param accepts
	 *            the HTTP Accepts header to use in retrieving Linked Data
	 *            resources
	 * @return this {@link DatasetCacheLoader} for further operation
	 */
	public DatasetCacheLoader accepts(final String accepts) {
		this.accepts = accepts;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see CacheLoader#cache(java.lang.Object)
	 */
	@Override
	public DatasetCacheLoader cache(final Dataset d) {
		this.dataset = d;
		return this;
	}

	private static Selector statementsWithUriObjects = new Selector() {

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
	};

	private static Function<RDFNode, Resource> cast = new Function<RDFNode, Resource>() {

		@Override
		public Resource apply(final RDFNode n) {
			return (Resource) n;
		}
	};
}
