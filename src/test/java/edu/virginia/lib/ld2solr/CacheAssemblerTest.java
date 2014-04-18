package edu.virginia.lib.ld2solr;

import static com.google.common.collect.Sets.difference;
import static com.hp.hpl.jena.query.ReadWrite.READ;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static com.hp.hpl.jena.tdb.TDBFactory.createDataset;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.impl.TestHelper;

public class CacheAssemblerTest extends TestHelper {

	private CacheAssembler testAssembler;

	private Dataset dataset;

	private static final Logger log = getLogger(CacheAssemblerTest.class);

	@Before
	public void setUp() {
		dataset = createDataset();
		testAssembler = new CacheAssembler(dataset, 3);
		testAssembler.uris(uris);
	}

	@Test
	public void testAccumulation() {
		final Set<Resource> successfullyRetrievedUris = testAssembler.call();
		assertEquals("Did not retrieve all resources successfully!", uris, successfullyRetrievedUris);
		dataset.begin(READ);
		log.debug("Retrieved triples: {}", dataset.getDefaultModel());
		for (final Resource uri : uris)
			assertTrue("Did not find an appropriate subject " + uri + " in triplestore!", dataset.getDefaultModel()
					.containsResource(uri));
		dataset.end();
	}

	@Test
	public void testAccumulationWithProblem() {
		final Set<Resource> urisWithExtra = new HashSet<>(uris);
		final Set<Resource> badUris = singleton(createResource());
		urisWithExtra.addAll(badUris);
		testAssembler = new CacheAssembler(dataset).uris(urisWithExtra);
		final Set<Resource> successfulUris = testAssembler.call();
		assertEquals("Didn't find the appropriate resource failing to be retrieved!", badUris,
				difference(urisWithExtra, successfulUris));
	}

}
