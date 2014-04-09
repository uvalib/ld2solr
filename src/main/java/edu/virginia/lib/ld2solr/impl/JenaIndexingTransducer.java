/**
 * 
 */
package edu.virginia.lib.ld2solr.impl;

import static com.google.common.base.Throwables.propagate;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.marmotta.ldpath.LDPath;
import org.apache.marmotta.ldpath.exception.LDPathParseException;
import org.slf4j.Logger;

import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.spi.IndexingTransducer;

/**
 * @author ajs6f
 * 
 */
public class JenaIndexingTransducer implements IndexingTransducer {

	private final String transformation;

	private final JenaBackend cache;

	private static final Logger log = getLogger(JenaIndexingTransducer.class);

	/**
	 * @param linkedDataCache
	 * @param transformationSource
	 */
	public JenaIndexingTransducer(final JenaBackend linkedDataCache, final String transformationSource) {
		this.transformation = transformationSource;
		this.cache = linkedDataCache;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.google.common.base.Function#apply(java.lang.Object)
	 */
	@Override
	public NamedFields apply(final Resource uri) {
		log.debug("Indexing: {}", uri);
		try (Reader transformationReader = new StringReader(transformation);) {
			return new NamedFields(new LDPath<>(cache).programQuery(uri, transformationReader));
		} catch (final LDPathParseException | IOException e) {
			throw propagate(e);
		}
	}

	/**
	 * @return the transformation
	 */
	public String transformation() {
		return transformation;
	}

	/**
	 * @return the cache
	 */
	public JenaBackend cache() {
		return cache;
	}

}
