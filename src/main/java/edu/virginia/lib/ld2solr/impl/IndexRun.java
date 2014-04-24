package edu.virginia.lib.ld2solr.impl;

import static com.google.common.util.concurrent.Futures.addCallback;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.spi.Stage;
import edu.virginia.lib.ld2solr.spi.ThreadedStage;

/**
 * A {@link Stage} of workflow that creates index records as {@link NamedFields}
 * units. Implements {@link Runnable} because it is usually separated from the
 * previous {@link Stage}s of cache accumulation. This is to enable users to
 * avoid (expensive, network-bound) retrieval operations if desired by beginning
 * workflow with this {@link Stage} over a pre-assembled cache.
 * 
 * @author ajs6f
 * 
 */
public class IndexRun extends ThreadedStage<IndexRun, NamedFields> implements Runnable {

	private final JenaBackend cache;

	private final String transformation;

	private final Set<Resource> uris;

	private final LDPathIndexer indexer;

	private static final Logger log = getLogger(IndexRun.class);

	/**
	 * @param transformationSource
	 *            LDPath transformation or program to use for indexing
	 * @param uris
	 *            {@link Resource}s to index
	 * @param c
	 *            Linked Data cache over which to operate
	 */
	public IndexRun(final String transformationSource, final Set<Resource> uris, final JenaBackend c) {
		this.transformation = transformationSource;
		this.uris = uris;
		this.cache = c;
		this.indexer = new LDPathIndexer(cache);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
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
					next(fields);
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
