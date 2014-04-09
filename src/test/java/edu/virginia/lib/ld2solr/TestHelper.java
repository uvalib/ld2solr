package edu.virginia.lib.ld2solr;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.google.common.base.Joiner.on;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.collect.Sets.newHashSet;
import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.slf4j.Logger;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.base.Function;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;

/**
 * 
 * Loads RDF resources into a local web server for testing purposes.
 * 
 * @author ajs6f
 * 
 */
public abstract class TestHelper {

	private static final Integer HTTP_PORT = 8089;

	@Rule
	public WireMockRule wireMockRule = new WireMockRule(HTTP_PORT);

	private static String uriBase = "http://localhost:" + HTTP_PORT + "/";

	protected static final String uriStump1 = "id/12345";
	protected static final String uriStump2 = "id/54321";
	protected static final String uriStump3 = "id/23456";

	protected static Set<Resource> uris = new HashSet<>();

	private static final Logger log = getLogger(TestHelper.class);

	@Before
	public void buildResources() throws FileNotFoundException, IOException {
		buildHttpResource("/" + uriStump1, "text/html", buildRdfaResponse("target/test-classes/ttl/" + uriStump1
				+ ".ttl"));

		buildHttpResource("/" + uriStump2, "text/html", buildRdfaResponse("target/test-classes/ttl/" + uriStump2
				+ ".ttl"));

		buildHttpResource("/" + uriStump3, "text/turtle", buildTtlResponse("target/test-classes/ttl/" + uriStump3
				+ ".ttl"));
	}

	private String buildRdfaResponse(final String fileName) throws FileNotFoundException, IOException {
		final Model m = retrieveSampleRdf(fileName);
		final String triples = on("").join(transform(m.listStatements(), statementToRdfaTriple));
		return rdfaPrefix + triples + rdfaSuffix;
	}

	private String buildTtlResponse(final String fileName) throws FileNotFoundException, IOException {
		final Model m = retrieveSampleRdf(fileName);
		try (StringWriter w = new StringWriter();) {
			m.write(w, "TURTLE");
			return w.toString();
		}
	}

	private static final Function<Statement, String> statementToRdfaTriple = new Function<Statement, String>() {

		@Override
		public String apply(final Statement stmnt) {
			final RDFNode object = stmnt.getObject();
			if (object.isResource()) {
				return "<p> There exists a thing named <span about=" + stmnt.getSubject() + "><a property=\""
						+ stmnt.getPredicate() + "\" " + (object.isResource() ? "href=\"" + object + "\"" : "") + ">"
						+ stmnt.getObject() + "</a></span>";
			} else {
				final Literal literal = object.asLiteral();
				if (literal.getDatatype() != null) {
					return "<p> There exists a thing named <span about=" + stmnt.getSubject() + "><a property=\""
							+ stmnt.getPredicate() + "\" datatype=\"" + literal.getDatatypeURI() + "\">"
							+ stmnt.getObject() + "</a></span>";
				}
			}
			return "<p> There exists a thing named <span about=" + stmnt.getSubject() + "><a property=\""
					+ stmnt.getPredicate() + "\">" + stmnt.getObject() + "</a></span>";
		}
	};

	private Model retrieveSampleRdf(final String fileName) throws FileNotFoundException, IOException {
		log.debug("Retrieving sample RDF from: {}", fileName);
		try (InputStream in = new FileInputStream(new File(fileName))) {
			final Model m = createDefaultModel().read(in, uriBase, "TURTLE");
			uris.addAll(newHashSet(m.listSubjects()));
			return m;
		}
	}

	private static final String rdfaPrefix = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			+ "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML+RDFa 1.0//EN\" "
			+ "\"http://www.w3.org/MarkUp/DTD/xhtml-rdfa-1.dtd\">"
			+ "<html xmlns=\"http://www.w3.org/1999/xhtml\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" version=\"XHTML+RDFa 1.0\" xml:lang=\"en\">"
			+ "<body> ";

	private static final String rdfaSuffix = "</body></html>";

	private void buildHttpResource(final String uri, final String mimeType, final String response) {
		stubFor(get(urlEqualTo(uri)).willReturn(
				aResponse().withStatus(200).withHeader("Content-Type", mimeType).withBody(response)));
	}

}
