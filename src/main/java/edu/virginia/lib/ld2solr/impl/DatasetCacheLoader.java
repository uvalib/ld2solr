package edu.virginia.lib.ld2solr.impl;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.hp.hpl.jena.query.ReadWrite.WRITE;
import static com.hp.hpl.jena.shared.Lock.READ;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.IdentifiedModel;
import edu.virginia.lib.ld2solr.spi.CacheLoader;
import edu.virginia.lib.ld2solr.spi.ThreadedStage;

public class DatasetCacheLoader extends ThreadedStage<DatasetCacheLoader, IdentifiedModel> implements
		CacheLoader<DatasetCacheLoader, Dataset> {

	private Dataset dataset;

	private final Set<Resource> successfullyLoaded = new HashSet<>();

	private static final Logger log = getLogger(DatasetCacheLoader.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see Acceptor#accept(java.lang.Object)
	 */
	@Override
	public void accept(final IdentifiedModel m) {
		final ListenableFuture<?> task = threadpool().submit(new Runnable() {

			private final Model model = m.model;

			@Override
			public void run() {
				if (!model.isEmpty()) {
					model.enterCriticalSection(READ);
					log.debug("Adding {} triples to cache...", model.size());
					log.trace("Adding triples: {}", model);
					try {
						final Boolean wasInTransaction = dataset.isInTransaction();
						if (!wasInTransaction) {
							dataset.begin(WRITE);
						}
						try {
							dataset.getDefaultModel().add(model);
							if (!wasInTransaction) {
								dataset.commit();
							}
							successfullyLoaded.add(m.uri);
							next(m);
						} catch (final Exception e) {
							log.error("Error adding triples to cache!");
							log.error("Triples: ", model);
							log.error("Exception: ", e);
						} finally {
							if (!wasInTransaction) {
								dataset.end();
							}
						}
					} finally {
						model.leaveCriticalSection();
						model.close();
					}
				}
			}
		});
		addCallback(task, new Callback(m));
	}

	private static class Callback implements FutureCallback<Object> {

		private final IdentifiedModel model;

		public Callback(final IdentifiedModel m) {
			this.model = m;
		}

		@Override
		public void onSuccess(final Object result) {
			log.debug("Successfully loaded resource: {}", model.uri);
		}

		@Override
		public void onFailure(final Throwable t) {
			log.error("Failed to load resource {}!", model.uri);
			log.error("With exception: ", t);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see CacheLoader#cache(java.lang.Object)
	 */
	@Override
	public DatasetCacheLoader cache(final Dataset d) {
		this.dataset = d;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see CacheLoader#successfullyLoaded()
	 */
	@Override
	public Set<Resource> successfullyLoaded() {
		return successfullyLoaded;
	}
}
