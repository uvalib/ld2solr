package edu.virginia.lib.ld2solr.impl;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Set;

import org.slf4j.Logger;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.IdentifiedModel;
import edu.virginia.lib.ld2solr.spi.CacheAssembler;
import edu.virginia.lib.ld2solr.spi.CacheLoader;
import edu.virginia.lib.ld2solr.spi.CacheRetriever;

/**
 * A {@link CacheAssembler} that loads Linked Data into a Jena {@link Dataset} .
 * 
 * @author ajs6f
 * 
 */
/**
 * @author ajs6f
 * 
 */
public class DatasetCacheAssembler implements CacheAssembler<DatasetCacheAssembler, Dataset> {

	private CacheRetriever cacheRetriever;
	private CacheLoader<?, Dataset> cacheLoader;

	private Dataset dataset;

	private static final long TIMEOUT = 10000;

	private static final long TIMESTEP = 1000;

	private static final Logger log = getLogger(DatasetCacheAssembler.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see CacheAssembler#load(java.util.Set)
	 */
	@Override
	public void assemble(final Set<Resource> uris) {
		cacheRetriever.andThen(cacheLoader);
		cacheLoader.andThen(new NoOp<IdentifiedModel, Void>());
		cacheLoader.cache(dataset);
		for (final Resource uri : uris) {
			log.info("Attempting to load: {}", uri);
			cacheRetriever.accept(uri);
		}
		wait(uris);
	}

	/**
	 * wait for certain resources to be resolved
	 * 
	 * @param uris
	 *            the {@link Resource}s on which to wait
	 */
	private void wait(final Set<Resource> uris) {
		final long startTime = currentTimeMillis();
		while (!cacheLoader.successfullyLoaded().containsAll(uris) && currentTimeMillis() < (startTime + TIMEOUT)) {
			try {
				log.trace("Waiting for {} to be fully loaded into {}...", uris, cacheLoader.successfullyLoaded());
				sleep(TIMESTEP);
			} catch (final InterruptedException e) {
				break;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see CacheAssembler#cache(java.lang.Object)
	 */
	@Override
	public DatasetCacheAssembler cache(final Dataset d) {
		this.dataset = d;
		return this;
	}

	/**
	 * @param cacheRetriever
	 *            the cacheRetriever to set
	 * @return this {@link DatasetCacheAssembler} for further operation
	 */
	public DatasetCacheAssembler cacheRetriever(final CacheRetriever cacheRetriever) {
		this.cacheRetriever = cacheRetriever;
		return this;
	}

	/**
	 * @param cacheLoader
	 *            the cacheLoader to set
	 * @return this {@link DatasetCacheAssembler} for further operation
	 */
	public DatasetCacheAssembler cacheLoader(final CacheLoader<?, Dataset> cacheLoader) {
		this.cacheLoader = cacheLoader;
		return this;
	}

	@Override
	public void shutdown() throws InterruptedException {
		cacheRetriever.shutdown();
		cacheLoader.shutdown();
	}

	@Override
	public Set<Resource> successfullyAssembled() {
		return cacheLoader.successfullyLoaded();
	}
}
