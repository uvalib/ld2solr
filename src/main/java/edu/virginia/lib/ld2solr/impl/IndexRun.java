/**
 * 
 */
package edu.virginia.lib.ld2solr.impl;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.hp.hpl.jena.query.ReadWrite.READ;
import static com.hp.hpl.jena.tdb.TDBFactory.createDataset;
import static java.lang.Byte.parseByte;
import static java.lang.System.getProperty;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.spi.AbstractStage;

/**
 * @author ajs6f
 * 
 */
public class IndexRun extends AbstractStage<NamedFields> implements Runnable {

	private JenaBackend cache;

	private final String transformation;

	private final Set<Resource> uris;

	private final LDPathIndexer indexer;

	private final byte numIndexerHeads;

	private static final byte defaultNumIndexerHeads = 10;

	private static final Logger log = getLogger(IndexRun.class);

	/**
	 * @param transformationSource
	 * @param uris
	 * @param cache
	 */
	public IndexRun(final String transformationSource, final Set<Resource> uris, final JenaBackend... caches) {
		this.transformation = transformationSource;
		this.uris = uris;
		if (caches.length > 0)
			this.cache = caches[0];
		else {
			final Dataset dataset = createDataset();
			dataset.begin(READ);
			this.cache = new JenaBackend(dataset.getDefaultModel());
			dataset.end();
		}
		this.indexer = new LDPathIndexer(cache);
		numIndexerHeads = parseByte(getProperty("numIndexerHeads", Byte.toString(defaultNumIndexerHeads)));
		threadpool = listeningDecorator(newFixedThreadPool(numIndexerHeads));
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
