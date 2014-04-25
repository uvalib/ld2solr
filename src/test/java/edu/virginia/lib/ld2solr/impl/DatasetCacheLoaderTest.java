package edu.virginia.lib.ld2solr.impl;

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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Resource;

public class DatasetCacheLoaderTest extends TestHelper {

	private DatasetCacheLoader testLoader;

	private Dataset dataset;

	private static final Logger log = getLogger(DatasetCacheLoaderTest.class);

	@Before
	public void setUp() throws InterruptedException {
		dataset = createDataset();
		testLoader = new DatasetCacheLoader().cache(dataset).threads(10);
	}
	
	@After
	public void tearDown() throws InterruptedException {
		testLoader.shutdown();
	}

	@Test
	public void testAccumulation() {
		final Set<Resource> successfullyRetrievedUris = testLoader.load(uris);
		assertTrue("Did not retrieve all resources successfully!", successfullyRetrievedUris.containsAll(uris));
		dataset.begin(READ);
		log.debug("Retrieved triples: {}", dataset.getDefaultModel());
		for (final Resource uri : uris)
			assertTrue("Did not find an appropriate subject " + uri + " in triplestore!", dataset.getDefaultModel()
					.containsResource(uri));
		dataset.end();
	}

	@Test
	public void testAccumulationWithEmptyResource() {
		final Set<Resource> urisWithEmpty = new HashSet<>(uris);
		urisWithEmpty.add(createResource(uriBase + "empty"));
		final Set<Resource> successfullyRetrievedUris = testLoader.load(urisWithEmpty);
		log.debug("Retrieved URIs: {}", successfullyRetrievedUris);
		assertTrue("Did not retrieve all resources successfully!", successfullyRetrievedUris.containsAll(uris));
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
		testLoader = new DatasetCacheLoader().cache(dataset);
		final Set<Resource> successfulUris = testLoader.load(urisWithExtra);
		assertEquals("Didn't find the appropriate resource failing to be retrieved!", badUris,
						difference(urisWithExtra, successfulUris));
		assertTrue("Did not retrieve all resources successfully that should have been retrieved!",
						successfulUris.containsAll(uris));

	}

}
