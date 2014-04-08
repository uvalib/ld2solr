/**
 * 
 */
package edu.virginia.lib.ld2solr.impl;

import java.io.Reader;

import org.apache.marmotta.ldpath.backend.jena.GenericJenaBackend;

import edu.virginia.lib.ld2solr.spi.Indexer;

/**
 * @author ajs6f
 * 
 */
public class LDPathIndexer implements Indexer<LDPathIndexerHead> {

	private final GenericJenaBackend cache;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.google.common.base.Function#apply(java.lang.Object)
	 */
	@Override
	public LDPathIndexerHead apply(final Reader transform) {
		return new LDPathIndexerHead(cache, transform);
	}

	/**
	 * @param linkedDataCache
	 *            the cache in which to store Linked Data to index
	 */
	public LDPathIndexer(final GenericJenaBackend linkedDataCache) {
		this.cache = linkedDataCache;
	}

}
