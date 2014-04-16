/**
 * 
 */
package edu.virginia.lib.ld2solr;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.JdkFutureAdapters.listenInPoolThread;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.hp.hpl.jena.shared.Lock.READ;
import static com.hp.hpl.jena.shared.Lock.WRITE;
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
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.impl.JenaModelTriplesRetriever;
import edu.virginia.lib.ld2solr.spi.AbstractStage;

/**
 * @author ajs6f
 * 
 */
public class CacheAssembler extends AbstractStage<Void> implements Callable<Set<Resource>> {

	private Integer numReaderThreads = 1;

	private final CompletionService<Model> internalQueue;

	private final Model model;

	private Set<Resource> successfullyLoadedResources;

	private final Set<Resource> uris;

	private static final Logger log = getLogger(CacheAssembler.class);

	public CacheAssembler(final Model m, final Set<Resource> uris, final Integer... threads) {
		this.model = m;
		this.uris = uris;
		if (threads.length > 0)
			numReaderThreads = threads[0];
		threadpool = listeningDecorator(newFixedThreadPool(numReaderThreads));
		this.internalQueue = new ExecutorCompletionService<Model>(threadpool);
	}

	/**
	 * @return a set of URIs of successfully loaded resources
	 */
	@Override
	public Set<Resource> call() {
		successfullyLoadedResources = new HashSet<>(uris.size());
		for (final Resource uri : uris) {
			log.debug("Retrieving URI: {}", uri);
			final Future<Model> loadFuture = internalQueue.submit(new JenaModelTriplesRetriever(uri));
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
		for (final Resource uri : uris) {
			try {
				final Model m = internalQueue.take().get();
				m.enterCriticalSection(READ);
				try {
					log.debug("Adding triples for resource: {}...", uri);
					synchronized (model) {
						model.enterCriticalSection(WRITE);
						try {
							model.add(m);
						} catch (final Exception e) {
							log.error("Error adding triples to cache!");
							log.error("Exception: ", e);
						} finally {
							model.leaveCriticalSection();
						}
					}
				} finally {
					m.leaveCriticalSection();
				}
			} catch (InterruptedException | ExecutionException e) {
				log.error("Error assembling triples to add to cache!");
				log.error("Exception: ", e);
			}

		}
		return successfullyLoadedResources;
	}

	@Override
	public void andThen(final Acceptor<Void, ?> a) {
		throw new UnsupportedOperationException();
	}

}
