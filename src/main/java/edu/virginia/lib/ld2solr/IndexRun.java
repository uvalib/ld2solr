/**
 * 
 */
package edu.virginia.lib.ld2solr;

import static com.google.common.base.Throwables.propagate;
import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static java.lang.Byte.parseByte;
import static java.lang.System.getProperty;
import static java.util.concurrent.Executors.newFixedThreadPool;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.apache.marmotta.ldpath.backend.jena.GenericJenaBackend;

import com.google.common.base.Supplier;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.impl.LDPathIndexer;

/**
 * @author ajs6f
 * 
 */
public class IndexRun implements Supplier<Iterator<NamedFields>> {

	private GenericJenaBackend cache;

	private final Reader transformation;

	private final Set<Resource> uris;

	private final LDPathIndexer indexer;

	private final byte numIndexerHeads;

	private static final byte defaultNumIndexerHeads = 10;

	private final ExecutorService threadpool;

	/**
	 * @param transformation
	 * @param uris
	 * @param cache
	 * @throws IOException
	 */
	public IndexRun(final Reader transformation, final Set<Resource> uris, final GenericJenaBackend... caches)
			throws IOException {
		this.transformation = transformation;
		this.uris = uris;
		if (caches.length > 0)
			this.cache = caches[0];
		else
			this.cache = new GenericJenaBackend(createDefaultModel());
		this.indexer = new LDPathIndexer(cache);
		numIndexerHeads = parseByte(getProperty("numIndexerHeads", Byte.toString(defaultNumIndexerHeads)));
		threadpool = newFixedThreadPool(numIndexerHeads);
	}

	@Override
	public Iterator<NamedFields> get() {
		return new Iterator<NamedFields>() {

			private final Iterator<Resource> records = uris.iterator();

			@Override
			public boolean hasNext() {
				return records.hasNext();
			}

			@Override
			public NamedFields next() {
				try {
					return threadpool.submit(new Callable<NamedFields>() {

						@Override
						public NamedFields call() throws Exception {
							return indexer.apply(transformation).apply(records.next());
						}
					}).get();
				} catch (InterruptedException | ExecutionException e) {
					throw propagate(e);
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

}
