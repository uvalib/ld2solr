package edu.virginia.lib.ld2solr.impl;

import static com.google.common.collect.Sets.difference;
import static com.hp.hpl.jena.query.ReadWrite.READ;
import static com.hp.hpl.jena.rdf.model.ModelFactory.createOntologyModel;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static com.hp.hpl.jena.tdb.TDBFactory.createDataset;
import static com.hp.hpl.jena.vocabulary.OWL.ObjectProperty;
import static com.hp.hpl.jena.vocabulary.RDF.type;
import static com.hp.hpl.jena.vocabulary.RDFS.subPropertyOf;
import static edu.virginia.lib.ld2solr.spi.CacheAssembler.traversableForRecursiveRetrieval;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.IdentifiedModel;
import edu.virginia.lib.ld2solr.spi.CacheLoader;

public class DatasetCacheAssemblerTest extends TestHelper {

	private DatasetCacheAssembler testLoader;

	private Dataset dataset;

	private static final Logger log = getLogger(DatasetCacheAssemblerTest.class);

	@Before
	public void setUp() {
		dataset = createDataset();
		testLoader = new DatasetCacheAssembler().cache(dataset);
		final CacheLoader<?, Dataset> datasetCacheLoader = new DatasetCacheLoader();
		datasetCacheLoader.andThen(new TestAcceptor<IdentifiedModel, Void>());
		testLoader.cacheRetriever(new Any23CacheRetriever()).cacheLoader(datasetCacheLoader);
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
		final Set<Resource> successfulUris = testLoader.load(urisWithExtra);
		assertEquals("Didn't find the appropriate resource failing to be retrieved!", badUris,
				difference(urisWithExtra, successfulUris));
		assertTrue("Did not retrieve all resources successfully that should have been retrieved!",
				successfulUris.containsAll(uris));

	}

	@Test
	@Ignore("Until ontological Feedforward stage ")
	public void testAccumulationWithRecursion() {
		final Set<Resource> urisSansOne = new HashSet<>(uris);
		final Resource toBeRecursivelyLoaded = createResource(uriBase + "recursive2");
		urisSansOne.remove(toBeRecursivelyLoaded);
		final OntModel schemaForRecursiveRetrieval = createOntologyModel();
		// we add dc:subject as a traversable property
		schemaForRecursiveRetrieval
				.createProperty("http://purl.org/dc/elements/1.1/", "subject")
				.addProperty(type, ObjectProperty)
				.addProperty(subPropertyOf,
						schemaForRecursiveRetrieval.createProperty(traversableForRecursiveRetrieval.getURI()));

		testLoader = new DatasetCacheAssembler().cache(dataset).ontology(schemaForRecursiveRetrieval);
		final Set<Resource> successfulUris = testLoader.load(urisSansOne);
		assertTrue("Did not retrieve all resources successfully that should have been retrieved!",
				successfulUris.containsAll(uris));
	}

}
