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
import org.junit.Test;
import org.slf4j.Logger;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Property;
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
		testLoader = new DatasetCacheAssembler().cache(dataset).timeout(5000);
		final CacheLoader<?, Dataset> datasetCacheLoader = new DatasetCacheLoader();
		// TODO make the setting of the datasetCacheLoader andThen more elegant
		datasetCacheLoader.andThen(new TestAcceptor<IdentifiedModel, Void>());
		final OntModel schemaForRecursiveRetrieval = createOntologyModel();
		// we create dc:subject as a recursively traversable property
		final Property dcSubject = schemaForRecursiveRetrieval.createProperty("http://purl.org/dc/elements/1.1/",
				"subject");
		dcSubject.addProperty(type, ObjectProperty);
		dcSubject.addProperty(subPropertyOf,
				schemaForRecursiveRetrieval.createProperty(traversableForRecursiveRetrieval.getURI()));

		testLoader
				.cacheRetriever(new Any23CacheRetriever())
				.cacheLoader(datasetCacheLoader)
				.cacheEnhancer(
						new RecursiveRetrievalEnhancer().ontology(schemaForRecursiveRetrieval).cacheAssembler(
								testLoader));
	}

	@After
	public void tearDown() throws InterruptedException {
		testLoader.shutdown();
	}

	@Test
	public void testAccumulation() {
		log.trace("Entering testAccumulation()...");
		testLoader.assemble(uris);
		final Set<Resource> successfullyRetrievedUris = testLoader.successfullyAssembled();
		log.debug("Assembled successfully: {}", successfullyRetrievedUris);
		assertTrue("Did not retrieve all resources successfully!", successfullyRetrievedUris.containsAll(uris));
		dataset.begin(READ);
		log.debug("Retrieved triples: {}", dataset.getDefaultModel());
		for (final Resource uri : uris)
			assertTrue("Did not find an appropriate subject " + uri + " in triplestore!", dataset.getDefaultModel()
					.contains(uri, null));
		dataset.end();
		log.trace("Leaving testAccumulation().");
	}

	@Test
	public void testAccumulationWithEmptyResource() {
		final Set<Resource> urisWithEmpty = new HashSet<>(uris);
		urisWithEmpty.add(createResource(uriBase + "empty"));
		testLoader.assemble(urisWithEmpty);
		final Set<Resource> successfullyRetrievedUris = testLoader.successfullyAssembled();
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
		testLoader.assemble(urisWithExtra);
		final Set<Resource> successfulUris = testLoader.successfullyAssembled();
		assertEquals("Didn't find the appropriate resource failing to be retrieved!", badUris,
				difference(urisWithExtra, successfulUris));
		assertTrue("Did not retrieve all resources successfully that should have been retrieved!",
				successfulUris.containsAll(uris));

	}

	@Test
	public void testAccumulationWithRecursion() {
		log.trace("Entering testAccumulationWithRecursion()...");
		final Set<Resource> urisSansOne = new HashSet<>(uris);
		final Resource toBeRecursivelyLoaded = createResource(uriBase + "recursive2");
		urisSansOne.remove(toBeRecursivelyLoaded);
		testLoader.assemble(urisSansOne);
		final Set<Resource> successfulUris = testLoader.successfullyAssembled();
		log.debug("Retrieved successfully: {}", successfulUris);
		dataset.begin(READ);
		log.debug("Retrieved triples: {}", dataset.getDefaultModel());
		dataset.end();
		assertTrue("Did not retrieve all resources successfully that should have been retrieved!",
				successfulUris.containsAll(uris));
		log.trace("Leaving testAccumulationWithRecursion().");
	}

}
