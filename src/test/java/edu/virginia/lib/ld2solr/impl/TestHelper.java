package edu.virginia.lib.ld2solr.impl;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.google.common.base.Joiner.on;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.collect.Sets.newHashSet;
import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static java.lang.Thread.sleep;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.TERMINATE;
import static java.nio.file.Files.walkFileTree;
import static org.apache.commons.lang.StringUtils.substringBefore;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
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

	/**
	 * Establishes a webserver able to support our test LD resources.
	 */
	public static final Integer HTTP_PORT = Integer.getInteger("test.port", 8089);
	@Rule
	public WireMockRule wireMockRule = new WireMockRule(HTTP_PORT);

	protected static final String SAMPLE_RDF = "target/test-classes/rdf/";

	protected static String uriBase = "http://localhost:" + HTTP_PORT + "/";

	protected static Set<Resource> uris = new HashSet<>();

	private static final Logger log = getLogger(TestHelper.class);

	@Before
	public void buildResources() throws FileNotFoundException, IOException, InterruptedException {
		uris = new HashSet<>();
		for (final File sampleDirectory : new File(SAMPLE_RDF).listFiles()) {
			final LDMediaType type = LDMediaType.valueOf(sampleDirectory.getName().toUpperCase());
			log.trace("Publishing sample data of type: {}", type);
			walkFileTree(sampleDirectory.toPath(), new LDResourceStubber(type));
		}
		// it takes a few moments for the resources to be present in our server
		while (!urisPresent()) {
			log.debug("Waiting for test resources to be established...");
			sleep(1000);
		}
	}

	private boolean urisPresent() {
		for (final Resource uri : uris) {
			try {
				new URL(uri.getURI()).getContent();
				log.trace("Tested retrieval of URI: {}", uri);
			} catch (final IOException e) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Used to walk the filesystem of sample RDF data and create testable Linked
	 * Data resources.
	 * 
	 * @author ajs6f
	 * 
	 */
	private static class LDResourceStubber implements FileVisitor<Path> {

		private final LDMediaType type;

		public LDResourceStubber(final LDMediaType t) {
			this.type = t;
		}

		@Override
		public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) {
			return CONTINUE;
		}

		@Override
		public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
			type.buildResource(file);
			return CONTINUE;
		}

		@Override
		public FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException {
			throw exc;
		}

		@Override
		public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) {
			return TERMINATE;
		}
	}

	protected static enum LDMediaType {
		RDFA {
			@Override
			public void buildResource(final Path fileName) throws FileNotFoundException, IOException {
				final String id = fileName.getFileName().toString();
				buildHttpResource("/" + substringBefore(id, "."), "text/html", buildRdfaResponse(fileName));
			}

			private String buildRdfaResponse(final Path fileName) throws FileNotFoundException, IOException {
				final Model m = retrieveSampleRdf(fileName);
				final String triples = on("").join(transform(m.listStatements(), statementToRdfaTriple));
				return rdfaPrefix + triples + rdfaSuffix;
			}

			private final Function<Statement, String> statementToRdfaTriple = new Function<Statement, String>() {

				@Override
				public String apply(final Statement stmnt) {
					final RDFNode object = stmnt.getObject();
					if (object.isResource()) {
						return "<p> There exists a thing named <span about=" + stmnt.getSubject() + "><a property=\""
								+ stmnt.getPredicate() + "\" href=\"" + object + "\"" + ">" + stmnt.getObject()
								+ "</a></span>";
					}
					final Literal literal = object.asLiteral();
					if (literal.getDatatype() != null) {
						return "<p> There exists a thing named <span about=" + stmnt.getSubject() + "><a property=\""
								+ stmnt.getPredicate() + "\" datatype=\"" + literal.getDatatypeURI() + "\">"
								+ stmnt.getObject().asLiteral().getLexicalForm() + "</a></span>";
					}
					return "<p> There exists a thing named <span about=" + stmnt.getSubject() + "><a property=\""
							+ stmnt.getPredicate() + "\">" + stmnt.getObject() + "</a></span>";
				}
			};
			private static final String rdfaPrefix = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
					+ "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML+RDFa 1.0//EN\" "
					+ "\"http://www.w3.org/MarkUp/DTD/xhtml-rdfa-1.dtd\">"
					+ "<html xmlns=\"http://www.w3.org/1999/xhtml\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" version=\"XHTML+RDFa 1.0\" xml:lang=\"en\">"
					+ "<body> ";

			private static final String rdfaSuffix = "</body></html>";
		},

		RDFXML {
			@Override
			public void buildResource(final Path fileName) throws FileNotFoundException, IOException {
				final String id = fileName.getFileName().toString();
				try (StringWriter w = new StringWriter()) {
					retrieveSampleRdf(fileName).write(w);
					buildHttpResource("/" + substringBefore(id, "."), "application/rdf+xml", w.toString());
				}
			}
		},

		TTL {
			@Override
			public void buildResource(final Path fileName) throws FileNotFoundException, IOException {
				final String id = fileName.getFileName().toString();
				final Model m = retrieveSampleRdf(fileName);
				try (StringWriter w = new StringWriter();) {
					m.write(w, "TTL");
					buildHttpResource("/" + substringBefore(id, "."), "text/turtle", w.toString());
				}
			}
		};

		public abstract void buildResource(Path fileName) throws FileNotFoundException, IOException;

		protected static Model retrieveSampleRdf(final Path fileName) throws FileNotFoundException, IOException {
			log.debug("Retrieving sample RDF from file: {}", fileName);
			try (InputStream in = new FileInputStream(fileName.toFile())) {
				final Model m = createDefaultModel().read(in, uriBase, "TTL");
				uris.addAll(newHashSet(m.listSubjects()));
				return m;
			}
		}

		private static void buildHttpResource(final String uri, final String mimeType, final String response) {
			stubFor(get(urlEqualTo(uri)).willReturn(
					aResponse().withStatus(200).withHeader("Content-Type", mimeType).withBody(response)));
		}

	}

}
