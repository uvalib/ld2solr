package edu.virginia.lib.ld2solr.impl;

import static com.google.common.base.Throwables.propagate;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.marmotta.ldpath.LDPath;
import org.apache.marmotta.ldpath.exception.LDPathParseException;
import org.apache.marmotta.ldpath.parser.DefaultConfiguration;
import org.slf4j.Logger;

import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.spi.IndexingTransducer;

/**
 * An {@link IndexingTransducer} that indexes from a {@link JenaBackend} Linked
 * Data store using an LDPath transformation.
 * 
 * @author ajs6f
 * 
 */
public class JenaIndexingTransducer implements IndexingTransducer {

	private final String transformation;

	private final JenaBackend cache;

	private static final Logger log = getLogger(JenaIndexingTransducer.class);

	/**
	 * @param linkedDataCache
	 *            the cache over which to operate
	 * @param transformationSource
	 *            a {@link String} containing the LDPath transform to apply
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
		log.debug("Indexing resource: {}", uri);
		try (Reader transformationReader = new StringReader(transformation);) {
			final Map<String, Collection<?>> result = new LDPath<>(cache, new DefaultConfiguration<RDFNode>() {
				@Override
				public Map<String, String> getNamespaces() {
					final Map<String, String> namespaces = new HashMap<>(super.getNamespaces());
					namespaces.putAll(cache.model().getNsPrefixMap());
					return namespaces;
				}
			}).programQuery(uri, transformationReader);
			log.trace("LDPath transform returned: {}", result);
			final NamedFields fields = new NamedFields(result);
			// the identifier of the record is the URI of the resource indexed
			fields.id(uri.getURI());
			log.trace("Created index fields: {}", fields);
			return fields;
		} catch (final LDPathParseException | IOException e) {
			throw propagate(e);
		}
	}
}
