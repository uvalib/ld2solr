package edu.virginia.lib.ld2solr;

import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

public class CacheAssemblerTest extends TestHelper {

	private CacheAssembler testAssembler;

	private Model inMemoryModel;

	private static final Logger log = getLogger(CacheAssemblerTest.class);

	@Before
	public void setUp() {
		inMemoryModel = createDefaultModel();
		testAssembler = new CacheAssembler(inMemoryModel, uris);
	}

	@Test
	public void testAccumulation() throws InterruptedException {
		assertEquals("Did not retrieve all resources successfully!", uris, testAssembler.call());
		log.debug("Retrieved triples: {}", inMemoryModel.getGraph());
		for (final Resource uri : uris)
			assertTrue("Did not find an appropriate subject " + uri + " in triplestore!",
					inMemoryModel.containsResource(uri));
	}

}
