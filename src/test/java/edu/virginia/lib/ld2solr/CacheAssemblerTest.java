package edu.virginia.lib.ld2solr;

import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

public class CacheAssemblerTest {

	private CacheAssembler testAssembler;

	private Model inMemoryModel;

	private static final Resource uri1 = createResource("file:target/test-classes/rdfa/urn:ISBN:12345.html");
	private static final Resource uri2 = createResource("file:target/test-classes/rdfa/urn:ISBN:54321.html");
	private static final Resource uri3 = createResource("file:target/test-classes/nt/urn:ISBN:23456.ttl");

	private static final Set<Resource> uris = ImmutableSet.of(uri1, uri2, uri3);

	private static final Logger log = getLogger(CacheAssemblerTest.class);

	@Before
	public void setUp() {
		inMemoryModel = createDefaultModel();
		testAssembler = new CacheAssembler(inMemoryModel, uris);
	}

	@Test
	public void testAccumulation() throws InterruptedException {
		assertEquals("Did not retrieve all triples successfully!", uris, testAssembler.call());
		log.debug("Retrieved triples: {}", inMemoryModel.getGraph());
		for (final Resource uri : uris)
			assertTrue("Did not find an appropriate subject " + uri + " in triplestore!",
					inMemoryModel.containsResource(uri));
	}

}
