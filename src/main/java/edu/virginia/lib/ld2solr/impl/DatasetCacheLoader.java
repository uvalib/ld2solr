package edu.virginia.lib.ld2solr.impl;

import static com.hp.hpl.jena.query.ReadWrite.WRITE;
import static com.hp.hpl.jena.shared.Lock.READ;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;

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
		threadpool().submit(new Runnable() {

			private final Model model = m.model;

			@Override
			public void run() {
				if (!model.isEmpty()) {
					model.enterCriticalSection(READ);
					log.debug("Adding {} triples to cache...", model.size());
					log.trace("Adding triples: {}", model);
					try {
						dataset.begin(WRITE);
						try {
							dataset.getDefaultModel().add(model);
							dataset.commit();
							successfullyLoaded.add(m.uri);
							next(m);
						} catch (final Exception e) {
							log.error("Error adding triples to cache!");
							log.error("Triples: ", model);
							log.error("Exception: ", e);
						} finally {
							dataset.end();
						}
					} finally {
						model.leaveCriticalSection();
						model.close();
					}
				}
			}
		});
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
