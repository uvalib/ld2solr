/**
 * 
 */
package edu.virginia.lib.ld2solr;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static java.lang.Byte.parseByte;
import static java.lang.System.getProperty;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.impl.JenaBackend;
import edu.virginia.lib.ld2solr.impl.LDPathIndexer;
import edu.virginia.lib.ld2solr.spi.Stage;

/**
 * @author ajs6f
 * 
 */
public class IndexRun implements Runnable, Stage<NamedFields> {

	private JenaBackend cache;

	private final String transformation;

	private final Set<Resource> uris;

	private final LDPathIndexer indexer;

	private final byte numIndexerHeads;

	private static final byte defaultNumIndexerHeads = 10;

	private final ListeningExecutorService threadpool;

	private Acceptor<NamedFields, ?> nextStage;

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
		else
			this.cache = new JenaBackend(createDefaultModel());
		this.indexer = new LDPathIndexer(cache);
		numIndexerHeads = parseByte(getProperty("numIndexerHeads", Byte.toString(defaultNumIndexerHeads)));
		threadpool = listeningDecorator(newFixedThreadPool(numIndexerHeads));
	}

	@Override
	public void run() {
		for (final Resource uri : uris) {
			log.info("Indexing {}...", uri);
			final ListenableFuture<NamedFields> result = threadpool.submit(new Callable<NamedFields>() {

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

	@Override
	public void andThen(final Acceptor<NamedFields, ?> s) {
		this.nextStage = s;
	}

}
