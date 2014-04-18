/**
 * 
 */
package edu.virginia.lib.ld2solr;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.JdkFutureAdapters.listenInPoolThread;
import static com.hp.hpl.jena.query.ReadWrite.WRITE;
import static com.hp.hpl.jena.shared.Lock.READ;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.impl.JenaModelTriplesRetriever;
import edu.virginia.lib.ld2solr.spi.Stage;

/**
 * @author ajs6f
 * 
 */
public class CacheAssembler implements Callable<Set<Resource>> {

	private Byte numReaderThreads = Stage.DEFAULT_NUM_THREADS;

	private final CompletionService<Model> internalQueue;

	private final Dataset dataset;

	private Set<Resource> successfullyLoadedResources;

	private Set<Resource> uris;

	private static final Logger log = getLogger(CacheAssembler.class);

	public CacheAssembler(final Dataset d, final Byte... threads) {
		this.dataset = d;
		if (threads.length > 0)
			numReaderThreads = threads[0];
		this.internalQueue = new ExecutorCompletionService<Model>(newFixedThreadPool(numReaderThreads));
	}

	/**
	 * @return a set of URIs of successfully loaded resources
	 */
	@Override
	public Set<Resource> call() {
		successfullyLoadedResources = new HashSet<>(uris.size());
		for (final Resource uri : uris) {
			log.debug("Queueing retrieval task for URI: {}...", uri);
			final Future<Model> loadFuture = internalQueue.submit(new JenaModelTriplesRetriever().apply(uri));
			final ListenableFuture<Model> loadTask = listenInPoolThread(loadFuture);
			addCallback(loadTask, new FutureCallback<Model>() {

				@Override
				public void onSuccess(final Model result) {
					log.debug("Retrieved URI: {} and will add its contents to cache.", uri);
					successfullyLoadedResources.add(uri);
				}

				@Override
				public void onFailure(final Throwable t) {
					log.error("Failed to retrieve: {}!", uri);
					log.error("Exception: ", t);
				}
			});
		}
		log.info("Finished queuing retrieval tasks.");
		// the only purpose of this loop is to ensure that we execute as many
		// tasks to add triples to the cache as we have executed tasks to
		// retrieve triples
		for (@SuppressWarnings("unused")
		final Resource uri : uris) {
			try {
				final Model m = internalQueue.take().get();
				log.debug("Adding {} triples to cache...", m.size());
				if (!m.isEmpty()) {
					m.enterCriticalSection(READ);
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
		return successfullyLoadedResources;
	}

	/**
	 * @param u
	 *            the uris to retrieve
	 * @return this {@link CacheAssembler} for chaining
	 */
	public CacheAssembler uris(final Set<Resource> u) {
		this.uris = u;
		return this;
	}
}
