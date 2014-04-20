package edu.virginia.lib.ld2solr.impl;

import static ch.qos.logback.classic.Level.INFO;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static edu.virginia.lib.ld2solr.impl.TestHelper.LDMediaType.retrieveSampleRdf;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;

import ch.qos.logback.classic.Level;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

public class JenaModelTriplesRetrieverTest extends TestHelper {

	private static final Path testFileName = Paths.get(SAMPLE_RDF + "ttl/23456.ttl");

	private static final Resource testUri = createResource(uriBase + "23456");

	private JenaModelTriplesRetriever testRetriever;

	@Before
	public void setUp() {
		testRetriever = new JenaModelTriplesRetriever(null);
	}

	@Test
	public void testRetrieval() throws Exception {
		final Model expected = retrieveSampleRdf(testFileName);
		assertTrue(expected.isIsomorphicWith(testRetriever.apply(testUri).call()));
	}

	@Test
	public void testRetrievalWithAccept() throws Exception {
		testRetriever = new JenaModelTriplesRetriever("nonsense-mime-type");
		final Model expected = retrieveSampleRdf(testFileName);
		assertTrue(expected.isIsomorphicWith(testRetriever.apply(testUri).call()));
	}

	@Test
	public void testRetrievalWithLessLogging() throws Exception {
		final ch.qos.logback.classic.Logger testLogger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
				.getLogger(JenaModelTriplesRetriever.class);
		final Level startLevel = testLogger.getLevel();
		try {
			testLogger.setLevel(INFO);
			Model expected = retrieveSampleRdf(testFileName);
			assertTrue(expected.isIsomorphicWith(testRetriever.apply(testUri).call()));
			testLogger.setLevel(Level.DEBUG);
			expected = retrieveSampleRdf(testFileName);
			assertTrue(expected.isIsomorphicWith(testRetriever.apply(testUri).call()));
		} finally {
			testLogger.setLevel(startLevel);
		}
	}

}
