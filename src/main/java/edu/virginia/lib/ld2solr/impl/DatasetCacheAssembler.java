package edu.virginia.lib.ld2solr.impl;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.IdentifiedModel;
import edu.virginia.lib.ld2solr.spi.CacheAssembler;
import edu.virginia.lib.ld2solr.spi.CacheEnhancer;
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

	private CacheEnhancer<?, Dataset> cacheEnhancer;

	private Dataset dataset;

	/**
	 * These are the resources this {@link DatasetCacheAssembler} has attempted
	 * to retrieve. We keep track of them to avoid trying to re-retrieve them.
	 * 
	 */
	private final Set<Resource> resourcesAttempted = new HashSet<>();;

	/**
	 * The amount of time to wait for a parcel of resources to fully load into
	 * cache.
	 */
	private long timeout = DEFAULT_TIMEOUT;

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
			if (!attempted().contains(uri)) {
				log.info("Attempting to load: {}", uri);
				resourcesAttempted.add(uri);
				cacheRetriever.accept(uri);
			} else {
				log.debug("{} has already been asked for.", uri);
			}

		}
		// wait until the newly requested resources have in fact been retrieved
		await(resourcesAttempted);
		cacheEnhancer.enhance();
	}

	/**
	 * wait for certain resources to be resolved
	 * 
	 * @param uris
	 *            the {@link Resource}s on which to wait
	 */
	private void await(final Set<Resource> uris) {
		final long startTime = currentTimeMillis();
		while (!cacheLoader.successfullyLoaded().containsAll(uris) && currentTimeMillis() < (startTime + timeout)) {
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
	 * @param cr
	 *            the {@link CacheRetriever} to use
	 * @return this {@link DatasetCacheAssembler} for further operation
	 */
	public DatasetCacheAssembler cacheRetriever(final CacheRetriever cr) {
		this.cacheRetriever = cr;
		return this;
	}

	/**
	 * @param cl
	 *            the {@link CacheLoader} to use
	 * @return this {@link DatasetCacheAssembler} for further operation
	 */
	public DatasetCacheAssembler cacheLoader(final CacheLoader<?, Dataset> cl) {
		this.cacheLoader = cl;
		return this;
	}

	/**
	 * @param ce
	 *            the {@link CacheEnhancer} to use
	 * @return this {@link DatasetCacheAssembler} for further operation
	 */
	public DatasetCacheAssembler cacheEnhancer(final CacheEnhancer<?, Dataset> ce) {
		this.cacheEnhancer = ce;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see CacheAssembler#shutdown()
	 */
	@Override
	public void shutdown() throws InterruptedException {
		cacheRetriever.shutdown();
		cacheLoader.shutdown();
		cacheEnhancer.shutdown();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see CacheAssembler#successfullyAssembled()
	 */
	@Override
	public Set<Resource> successfullyAssembled() {
		return cacheLoader.successfullyLoaded();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see CacheAssembler#cache()
	 */
	@Override
	public Dataset cache() {
		return dataset;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see CacheAssembler#attempted()
	 */
	@Override
	public Set<Resource> attempted() {
		return resourcesAttempted;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see CacheAssembler#timeout(long)
	 */
	@Override
	public DatasetCacheAssembler timeout(final long t) {
		timeout = t;
		return this;
	}
}
