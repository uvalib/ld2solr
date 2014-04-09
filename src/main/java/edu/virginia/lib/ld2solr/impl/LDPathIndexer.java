/**
 * 
 */
package edu.virginia.lib.ld2solr.impl;

import edu.virginia.lib.ld2solr.spi.Indexer;

/**
 * @author ajs6f
 * 
 */
public class LDPathIndexer implements Indexer<JenaIndexingTransducer> {

	private final JenaBackend cache;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.google.common.base.Function#apply(java.lang.Object)
	 */
	@Override
	public JenaIndexingTransducer apply(final String transform) {
		return new JenaIndexingTransducer(cache, transform);
	}

	/**
	 * @param linkedDataCache
	 *            the cache in which to store Linked Data to index
	 */
	public LDPathIndexer(final JenaBackend linkedDataCache) {
		this.cache = linkedDataCache;
	}

}
