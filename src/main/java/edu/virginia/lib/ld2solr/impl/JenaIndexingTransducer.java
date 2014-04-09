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
import edu.virginia.lib.ld2solr.spi.IndexingTransducer;

/**
 * @author ajs6f
 * 
 */
public class JenaIndexingTransducer implements IndexingTransducer {

	private final Reader transformation;

	private final JenaBackend cache;

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

	public JenaIndexingTransducer(final JenaBackend linkedDataCache, final Reader transformationSource) {
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
