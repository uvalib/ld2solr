/**
 * 
 */
package edu.virginia.lib.ld2solr.impl;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.spi.AbstractStage;

/**
 * @author ajs6f
 * 
 */
public class IndexRun extends AbstractStage<NamedFields> implements Runnable {

	private final JenaBackend cache;

	private final String transformation;

	private final Set<Resource> uris;

	private final LDPathIndexer indexer;

	private Integer numIndexerThreads = DEFAULT_NUM_THREADS;

	private static final Logger log = getLogger(IndexRun.class);

	/**
	 * @param transformationSource
	 * @param uris
	 * @param cache
	 */
	public IndexRun(final String transformationSource, final Set<Resource> uris, final JenaBackend c,
			final Integer... threads) {
		this.transformation = transformationSource;
		this.uris = uris;
		this.cache = c;
		this.indexer = new LDPathIndexer(cache);
		if (threads.length > 0) {
			numIndexerThreads = threads[0];
		}
		threadpool = listeningDecorator(newFixedThreadPool(numIndexerThreads));
	}

	@Override
	public void run() {
		for (final Resource uri : uris) {
			log.info("Indexing {}...", uri);
			final ListenableFuture<NamedFields> result = threadpool().submit(new Callable<NamedFields>() {

				@Override
				public NamedFields call() {
					return indexer.apply(transformation).apply(uri);
				}
			});
			addCallback(result, new FutureCallback<NamedFields>() {

				@Override
				public void onSuccess(final NamedFields fields) {
					log.info("Finished indexing: {}", uri);
					log.debug("With result: {}", fields);
					nextStage.accept(fields);
				}

				@Override
				public void onFailure(final Throwable t) {
					log.error("Failed to index: {}!", uri);
					log.error("With exception: ", t);
				}
			});
		}
	}

}
