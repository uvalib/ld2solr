package edu.virginia.lib.ld2solr.impl;

import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.any23.Any23;
import org.apache.any23.extractor.ExtractionContext;
import org.apache.any23.extractor.ExtractionException;
import org.apache.any23.writer.TripleHandler;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.slf4j.Logger;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.spi.TriplesRetriever;

/**
 * A {@link TriplesRetriever} that puts its retrieved triples into a Jena
 * {@link Model}.
 * 
 * @author ajs6f
 * 
 */
public class JenaModelTriplesRetriever implements TriplesRetriever {

	private static final String DEFAULT_USER_AGENT = "UVa Library Linked Data indexing engine";

	private final Any23 extractor = new Any23();

	private static final Logger log = getLogger(JenaModelTriplesRetriever.class);

	public JenaModelTriplesRetriever() {
		this.extractor.setHTTPUserAgent(DEFAULT_USER_AGENT);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.virginia.lib.ld2solr.impl.TriplesRetriever#load(com.hp.hpl.jena.rdf
	 * .model.Resource)
	 */
	@Override
	public Callable<Model> apply(final Resource uri) {
		return new Callable<Model>() {

			@Override
			public Model call() throws IOException, ExtractionException {
				log.debug("Retrieving from URI: {}", uri.getURI());
				final Model model = createDefaultModel();
				try (final TriplesIntoModel tripleRecorder = new TriplesIntoModel(model);) {
					extractor.extract(uri.getURI(), tripleRecorder);
				}
				return model;
			}
		};
	}

	/**
	 * Writes extracted triples into a Jena {@link Model}.
	 * 
	 * @author ajs6f
	 * 
	 */
	private static class TriplesIntoModel implements CloseableTripleHandler {

		private final Model model;

		public TriplesIntoModel(final Model m) {
			this.model = m;
		}

		@Override
		public void startDocument(final URI documentURI) {
		}

		@Override
		public void openContext(final ExtractionContext context) {
		}

		@Override
		public void receiveTriple(final org.openrdf.model.Resource s, final URI p, final Value o, final URI g,
				final ExtractionContext context) {
			final Property property = model.createProperty(p.stringValue());
			final Resource subject = model.createResource(s.stringValue());
			if (o instanceof org.openrdf.model.Literal) {
				final org.openrdf.model.Literal rawLiteral = (org.openrdf.model.Literal) o;
				final URI datatype = rawLiteral.getDatatype();
				final Literal literal;
				if (datatype != null) {
					literal = model.createTypedLiteral(rawLiteral.getLabel(), datatype.toString());
				} else {
					literal = model.createLiteral(rawLiteral.getLabel());
				}
				model.add(subject, property, literal);
			} else {
				final Resource resource = model.createResource(o.toString());
				model.add(subject, property, resource);
			}

		}

		@Override
		public void receiveNamespace(final String prefix, final String uri, final ExtractionContext context) {
			model.setNsPrefix(prefix, uri);
		}

		@Override
		public void closeContext(final ExtractionContext context) {
		}

		@Override
		public void endDocument(final URI documentURI) {
		}

		@Override
		public void setContentLength(final long contentLength) {
		}

		@Override
		public void close() {
		}

	}

	private static interface CloseableTripleHandler extends TripleHandler, Closeable {
	}

}
