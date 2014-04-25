package edu.virginia.lib.ld2solr.impl;

import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.apache.http.HttpStatus.SC_OK;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.any23.Any23;
import org.apache.any23.ExtractionReport;
import org.apache.any23.extractor.ExtractionContext;
import org.apache.any23.extractor.ExtractionException;
import org.apache.any23.extractor.Extractor;
import org.apache.any23.extractor.IssueReport.Issue;
import org.apache.any23.writer.TripleHandler;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.slf4j.Logger;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.spi.TriplesRetriever;

/**
 * A {@link TriplesRetriever} that returns its retrieved triples in an in-memory
 * Jena {@link Model}.
 * 
 * @author ajs6f
 * 
 */
public class JenaModelTriplesRetriever implements TriplesRetriever {

	private static final String DEFAULT_USER_AGENT = "UVa Library Linked Data indexing engine";

	private final Any23 extractor = new Any23();

	private static final HttpClient client = new DefaultHttpClient(new PoolingClientConnectionManager());

	private final String accept;

	private static final Logger log = getLogger(JenaModelTriplesRetriever.class);

	/**
	 * @param a
	 *            The HTTP Accept: header value to use in retrieval
	 */
	public JenaModelTriplesRetriever(final String a) {
		this.accept = a;
		this.extractor.setHTTPUserAgent(DEFAULT_USER_AGENT);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see TriplesRetriever#load(com.hp.hpl.jena.rdf .model.Resource)
	 */
	@Override
	public Callable<Model> apply(final Resource uri) {
		return new Callable<Model>() {

			@Override
			public Model call() throws IOException, ExtractionException {
				
				final String resource = uri.getURI();
				final Model model = createDefaultModel();
				
				log.debug("Retrieving from URI: {}", resource);
				final HttpGet get = new HttpGet(resource);
				if (accept != null) {
					get.setHeader("Accept", accept);
				}
				final HttpResponse response = client.execute(get);
				final int statusCode = response.getStatusLine().getStatusCode();
				if (statusCode != SC_OK) {
					log.error("Failed to retrieve: {}!", resource);
					throw new RetrievalException("Failed to retrieve: " + resource + "! HTTP Status code: "
									+ statusCode + ".");
				}
				
				final String rdf = EntityUtils.toString(response.getEntity());
				log.debug("Retrieved from URI: {} RDF:\n{}", resource, rdf);
				
				try (final TriplesIntoModel tripleRecorder = new TriplesIntoModel(model);) {
					final ExtractionReport report = extractor.extract(rdf, resource, tripleRecorder);
					if (log.isDebugEnabled()) {
						for (final Extractor<?> extractor : report.getMatchingExtractors()) {
							for (final Issue issue : report.getExtractorIssues(extractor.getDescription()
									.getExtractorName())) {
								log.debug("Extraction issue in {}: {} at: {}",
										new Object[] { resource, issue.getMessage(), issue.getRow() });
							}
						}
					}
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
	private static class TriplesIntoModel implements TripleHandler, Closeable {

		private final Model model;

		/**
		 * @param m
		 *            The {@link Model} into which to write
		 */
		public TriplesIntoModel(final Model m) {
			this.model = m;
		}

		@Override
		public void startDocument(final URI documentURI) {
		}

		@Override
		public void openContext(final ExtractionContext context) {
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.any23.writer.TripleHandler#receiveTriple(org.openrdf.model
		 * .Resource, org.openrdf.model.URI, org.openrdf.model.Value,
		 * org.openrdf.model.URI, org.apache.any23.extractor.ExtractionContext)
		 */
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

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.any23.writer.TripleHandler#receiveNamespace(java.lang.
		 * String, java.lang.String,
		 * org.apache.any23.extractor.ExtractionContext)
		 */
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
}
