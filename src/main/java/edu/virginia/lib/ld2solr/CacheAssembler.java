/**
 * 
 */
package edu.virginia.lib.ld2solr;

import static com.google.common.collect.Collections2.transform;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.any23.extractor.ExtractionException;
import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.impl.JenaTriplesRetriever;

/**
 * @author ajs6f
 * 
 */
public class CacheAssembler implements Callable<Set<Resource>> {

	private Integer numReaderThreads = 1;

	private final ListeningExecutorService threadpool;

	private final Model model;

	private final Set<Resource> successfullyLoadedResources = new HashSet<>();

	private final Set<Resource> uris;

	private static final Logger log = getLogger(CacheAssembler.class);

	public CacheAssembler(final Model m, final Set<Resource> uris, final Integer... threads) {
		this.model = m;
		this.uris = uris;
		if (threads.length > 0)
			numReaderThreads = threads[0];
		threadpool = listeningDecorator(newFixedThreadPool(numReaderThreads));
	}

	/**
	 * @param uris
	 *            a set of resource URIs to load
	 * @return a set of URIs of successfully loaded resources
	 * @throws InterruptedException
	 */
	@Override
	public Set<Resource> call() throws InterruptedException {
		final List<Future<Resource>> results = threadpool.invokeAll(transform(uris, createLoadingTask));
		// because invokeAll() above blocks until completion, the following
		// callbacks will execute immediately. see Futures.addCallback()
		// documentation
		for (final Future<Resource> result : results)
			addCallback((ListenableFuture<Resource>) result, new FutureCallback<Resource>() {

				@Override
				public void onSuccess(final Resource uri) {
					successfullyLoadedResources.add(uri);
				}

				@Override
				public void onFailure(final Throwable t) {
					log.error("Resource failed to load!", t);
				}
			});
		return successfullyLoadedResources;
	}

	private final Function<Resource, Callable<Resource>> createLoadingTask = new Function<Resource, Callable<Resource>>() {

		@Override
		public Callable<Resource> apply(final Resource uri) {
			return new Callable<Resource>() {

				@Override
				public Resource call() throws IOException, ExtractionException {
					return new JenaTriplesRetriever(model).load(uri);
				}
			};
		}
	};

}
