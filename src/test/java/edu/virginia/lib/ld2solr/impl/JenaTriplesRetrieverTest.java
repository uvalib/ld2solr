package edu.virginia.lib.ld2solr.impl;

import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;

import org.apache.any23.extractor.ExtractionException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.spi.TriplesRetriever;

public class JenaTriplesRetrieverTest extends TestHelper {

	private TriplesRetriever testRetriever;

	private Model model;

	private static final Logger log = getLogger(JenaTriplesRetrieverTest.class);

	@Before
	public void setUp() {
		model = createDefaultModel();
		testRetriever = new JenaTriplesRetriever(model);
	}

	@Test
	public void testRetrieval() throws IOException, ExtractionException {
		for (final Resource uri : uris) {
			log.debug("Attempting to retrieve from uri: {}", uri);
			testRetriever.load(uri);
		}
		log.debug("Retrieved triples: {}", model.getGraph());
		for (final Resource uri : uris) {
			assertTrue("Did not find triples containing " + uri + " in retrieved triples!", model.containsResource(uri));
		}

	}
}
