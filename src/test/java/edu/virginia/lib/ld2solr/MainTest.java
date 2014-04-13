package edu.virginia.lib.ld2solr;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Sets.difference;
import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Set;

import org.apache.marmotta.ldpath.LDPath;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.TestAcceptor.TestSink;
import edu.virginia.lib.ld2solr.api.OutputRecord;
import edu.virginia.lib.ld2solr.spi.OutputStage;

/**
 * 
 * @author ajs6f
 * 
 */
public class MainTest extends TestHelper {

	private Main testMain;

	private TestSink testSink;

	private OutputStage<?> testOutputStage;

	private static final String transformation = "title = dc:title :: xsd:string;\n"
			+ "alt_id = dc:identifier :: xsd:string;";

	private static final long TIMEOUT = 10000;

	private static final long TIMESTEP = 1000;

	@Before
	public void setUp() {
		/*
		 * because JUnit uses reflection to set up the classpath for a test and
		 * LDPath uses java.util.ServiceLoader to search the context classpath,
		 * we need to explicitly call out LDPath here to make sure it gets
		 * initialized properly
		 */
		new LDPath<String>(null);
		testMain = new Main();
		testMain.model(createDefaultModel());
		testSink = new TestSink();
		testMain.persister(testSink);
		testOutputStage = new TestOutputStage();
		testMain.outputStage(testOutputStage);
	}

	@Test
	public void testFullRun() throws InterruptedException {
		testMain.fullRun(transformation, uris, new HashSet<Resource>(uris.size()));
		final long startTime = currentTimeMillis();
		synchronized (testSink) {
			while (testSink.accepted().isEmpty() && currentTimeMillis() < (startTime + TIMEOUT)) {
				testSink.wait(TIMESTEP);
			}
		}
		for (final Resource uri : uris) {
			assertTrue("Failed to find a record for " + uri + " in output records: " + testSink.accepted() + "!",
					Iterables.any(testSink.accepted(), new Predicate<OutputRecord>() {

						@Override
						public boolean apply(final OutputRecord record) {

							return record.id().equals(uri.toString());
						}
					}));
		}
	}

	@Test
	public void testRunWithUnloadableResources() throws InterruptedException {
		final Set<Resource> urisWithExtra = new HashSet<>(uris);
		final Set<Resource> badUris = singleton(createResource());
		urisWithExtra.addAll(badUris);
		final Set<Resource> successfulUris = new HashSet<>();
		testMain.fullRun(transformation, urisWithExtra, successfulUris);
		final long startTime = currentTimeMillis();
		synchronized (testSink) {
			while (testSink.accepted().isEmpty() && currentTimeMillis() < (startTime + TIMEOUT)) {
				testSink.wait(TIMESTEP);
			}
		}
		assertEquals("Didn't find the appropriate resource failing to be retrieved!", badUris,
				difference(urisWithExtra, successfulUris));
	}

	@Test
	public void testMainMethodExecutes() {

		try {
			new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						Main.main(new String[0]);
					} catch (final Exception e) {
						propagate(e);
					}

				}
			}).run();
		} catch (final Exception e) {
			fail("Failed to execute Main.main with exception: " + e);
		}

	}
}
