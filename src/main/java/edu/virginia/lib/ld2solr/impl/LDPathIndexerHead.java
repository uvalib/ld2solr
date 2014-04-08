/**
 * 
 */
package edu.virginia.lib.ld2solr.impl;

import static com.google.common.base.Throwables.propagate;

import java.io.Reader;

import org.apache.marmotta.ldpath.LDPath;
import org.apache.marmotta.ldpath.backend.jena.GenericJenaBackend;
import org.apache.marmotta.ldpath.exception.LDPathParseException;

import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.spi.IndexerHead;

/**
 * @author ajs6f
 * 
 */
public class LDPathIndexerHead implements IndexerHead {

	private final Reader transformation;
	private final GenericJenaBackend cache;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.google.common.base.Function#apply(java.lang.Object)
	 */
	@Override
	public NamedFields apply(final Resource input) {
		try {
			return new NamedFields(new LDPath<>(cache).programQuery(input, transformation));
		} catch (final LDPathParseException e) {
			throw propagate(e);
		}
	}

	public LDPathIndexerHead(final GenericJenaBackend linkedDataCache, final Reader transformationSource) {
		this.transformation = transformationSource;
		this.cache = linkedDataCache;
	}

	/**
	 * @return the transformation
	 */
	public Reader transformation() {
		return transformation;
	}

	/**
	 * @return the cache
	 */
	public GenericJenaBackend cache() {
		return cache;
	}

}
