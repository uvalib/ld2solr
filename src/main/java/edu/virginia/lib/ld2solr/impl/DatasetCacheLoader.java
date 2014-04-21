package edu.virginia.lib.ld2solr.impl;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.hp.hpl.jena.query.ReadWrite.WRITE;
import static org.slf4j.LoggerFactory.getLogger;

import org.slf4j.Logger;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;

import edu.virginia.lib.ld2solr.spi.AbstractStage;
import edu.virginia.lib.ld2solr.spi.CacheLoader;

/**
 * A {@link CacheLoader} that loads RDF into a Jena {@link Dataset}.
 * 
 * @author ajs6f
 * 
 */
public class DatasetCacheLoader extends AbstractStage<DatasetCacheLoader, Void> implements CacheLoader {

	private Dataset dataset;

	private static final Logger log = getLogger(CacheAssembler.class);

	@Override
	public void accept(final Model additions) {
		final ListenableFuture<?> task = threadpool().submit(new Runnable() {
			@Override
			public void run() {
				dataset.begin(WRITE);
				try {
					dataset.getDefaultModel().add(additions);
					dataset.commit();
				} finally {
					dataset.end();
				}
			}
		});
		addCallback(task, new ReportCallback(additions));
	}

	private static class ReportCallback implements FutureCallback<Object> {

		private final Model model;

		public ReportCallback(final Model m) {
			this.model = m;
		}

		@Override
		public void onSuccess(final Object result) {
			log.debug("Added {} triples to cache...", model.size());
			log.trace("Added triples: {}", model);
		}

		@Override
		public void onFailure(final Throwable t) {
			log.error("Error assembling triples to add to cache!");
			log.error("Exception: ", t);
		}

	}

	/**
	 * @param dataset
	 *            the {@link Dataset} with which to back this loader
	 * @return this {@link DatasetCacheLoader} for continued operation
	 */
	public DatasetCacheLoader dataset(final Dataset d) {
		this.dataset = d;
		return this;
	}
}
