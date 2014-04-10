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

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.impl.JenaBackend;
import edu.virginia.lib.ld2solr.impl.LDPathIndexer;

/**
 * @author ajs6f
 * 
 */
public class IndexRun implements Supplier<Iterator<Future<NamedFields>>> {

	private JenaBackend cache;

	private final String transformation;

	private final Set<Resource> uris;

	private final LDPathIndexer indexer;

	private final byte numIndexerHeads;

	private static final byte defaultNumIndexerHeads = 10;

	private final ListeningExecutorService threadpool;

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
	public Iterator<Future<NamedFields>> get() {
		return new Iterator<Future<NamedFields>>() {

			private final Iterator<Resource> records = uris.iterator();

			@Override
			public boolean hasNext() {
				return records.hasNext();
			}

			@Override
			public Future<NamedFields> next() {
				final Resource uri = records.next();
				log.info("Indexing {}...", uri);
				final ListenableFuture<NamedFields> result = threadpool.submit(new Callable<NamedFields>() {

					@Override
					public NamedFields call() {
						return indexer.apply(transformation).apply(uri);
					}
				});
				addCallback(result, new SuccessReporter(uri));
				return result;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	private static class SuccessReporter implements FutureCallback<NamedFields> {

		private final Resource uri;

		public SuccessReporter(final Resource u) {
			this.uri = u;
		}

		@Override
		public void onSuccess(final NamedFields result) {
			log.debug("Finished indexing: {}", uri);
			log.debug("With result: {}", result);
		}

		@Override
		public void onFailure(final Throwable t) {
			log.error("Failed to index: {}!", uri);
			log.error("With exception: ", t);
		}
	}
}
