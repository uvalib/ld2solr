package edu.virginia.lib.ld2solr;

import static com.google.common.collect.Sets.difference;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.rdf.model.Resource;

public class MainTest extends TestHelper {

	private Main testMain;

	private static final String transformation = "title = dc:title :: xsd:string; id = dc:identifier :: xsd:string;";

	@Before
	public void setUp() {
		testMain = new Main();
	}

	@Test
	public void testFullRun() throws InterruptedException {
		testMain.fullRun(transformation, uris, new HashSet<Resource>());
	}

	@Test
	public void testRunWithBadResources() throws InterruptedException {
		final Set<Resource> urisWithExtra = new HashSet<>(uris);
		final Set<Resource> badUris = singleton(createResource());
		urisWithExtra.addAll(badUris);
		final Set<Resource> successfulUris = new HashSet<>();
		testMain.fullRun(transformation, urisWithExtra, successfulUris);
		assertEquals("Didn't find the appropriate resource failing to be indexed!", badUris,
				difference(urisWithExtra, successfulUris));
	}

}
