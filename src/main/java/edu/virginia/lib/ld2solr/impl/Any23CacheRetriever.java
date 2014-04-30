package edu.virginia.lib.ld2solr.impl;

import static com.google.common.collect.Sets.union;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.JdkFutureAdapters.listenInPoolThread;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.IdentifiedModel;
import edu.virginia.lib.ld2solr.spi.CacheRetriever;
import edu.virginia.lib.ld2solr.spi.ThreadedStage;

/**
 * A {@link CacheRetriever} that uses <a href="https://any23.apache.org/">Apache
 * Any23</a> to parse the retrieved triples.
 * 
 * @author ajs6f
 * 
 */
public class Any23CacheRetriever extends ThreadedStage<Any23CacheRetriever, IdentifiedModel> implements CacheRetriever {

	private final Set<Resource> successfullyRetrieved = new HashSet<>();

	private final Set<Resource> unsuccessfullyRetrieved = new HashSet<>();

	/**
	 * We use attemptedResources to determine whether or not to retrieve a
	 * resource, specifically, to avoid retrieving resources more than once. We
	 * make fullyAttemptedResources available for downstream stages to determine
	 * when a cycle of recursive retrieval is complete.
	 */
	private final Set<Resource> attemptedResources = new HashSet<>();
	private final Set<Resource> fullyAttemptedResources = union(unsuccessfullyRetrieved, successfullyRetrieved);

	private String accepts = null;

	private static final Logger log = getLogger(Any23CacheRetriever.class);

	@Override
	public void accept(final Resource uri) {
		log.debug("Attempting to load: {}", uri);

		if (attemptedResources.contains(uri)) {
			log.debug("{} has already been attempted, will not be re-attempted.", uri);
			return;
		}

		log.info("Queueing retrieval task for URI: {}...", uri);
		attemptedResources.add(uri);

		final Future<Model> loadFuture = threadpool().submit(new JenaModelTriplesRetriever(accepts).apply(uri));
		final ListenableFuture<Model> loadTask = listenInPoolThread(loadFuture);
		addCallback(loadTask, new FutureCallback<Model>() {

			@Override
			public void onSuccess(final Model result) {
				log.info("Retrieved URI: {}.", uri);
				successfullyRetrieved.add(uri);
				next(new IdentifiedModel(uri, result));
			}

			@Override
			public void onFailure(final Throwable t) {
				log.error("Failed to retrieve: {}!", uri);
				log.error("Exception: ", t);
				unsuccessfullyRetrieved.add(uri);
			}
		});

	}

	/**
	 * @return the resources we have either failed or succeeded to retrieve
	 */
	public Set<Resource> getFullyAttemptedResources() {
		return fullyAttemptedResources;
	}

	/**
	 * @return the successfullyRetrieved
	 */
	@Override
	public Set<Resource> successfullyRetrieved() {
		return successfullyRetrieved;
	}

	/**
	 * @param accepts
	 *            the HTTP Accept: header-value to use in HTTP retrieval
	 * @return this {@link Any23CacheRetriever} for further operation
	 */
	public Any23CacheRetriever accepts(final String accepts) {
		this.accepts = accepts;
		return this;
	}

}
