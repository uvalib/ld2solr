package edu.virginia.lib.ld2solr;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Joiner.on;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.ObjectArrays.concat;
import static com.google.common.collect.Sets.difference;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.Files.write;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static com.hp.hpl.jena.tdb.TDBFactory.createDataset;
import static edu.virginia.lib.ld2solr.Workflow.main;
import static java.io.File.createTempFile;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singleton;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.virginia.lib.ld2solr.api.OutputRecord;
import edu.virginia.lib.ld2solr.impl.TestAcceptor.TestSink;
import edu.virginia.lib.ld2solr.impl.TestHelper;
import edu.virginia.lib.ld2solr.impl.TestOutputStage;
import edu.virginia.lib.ld2solr.spi.OutputStage;

/**
 * 
 * @author ajs6f
 * 
 */
public class WorkflowTest extends TestHelper {

	private Workflow testMain;

	private TestSink testSink;

	private OutputStage testOutputStage;

	private CacheAssembler testAssembler;

	private static final String transformation = "title = dc:title :: xsd:string;\n"
			+ "alt_id = dc:identifier :: xsd:string;";

	private static final long TIMEOUT = 10000;

	private static final long TIMESTEP = 1000;

	private static final Logger log = getLogger(WorkflowTest.class);

	@Before
	public void setUp() {
		testMain = new Workflow();
		final Dataset dataset = createDataset();
		testMain.dataset(dataset);
		testSink = new TestSink();
		testMain.persister(testSink);
		testAssembler = new CacheAssembler(dataset).uris(uris);
		testMain.assembler(testAssembler);
		testOutputStage = new TestOutputStage();
		testMain.outputStage(testOutputStage);
	}

	@After
	public void tearDown() throws InterruptedException {
		testAssembler.shutdown();
		testOutputStage.shutdown();
		testSink.shutdown();
	}

	@Test
	public void testFullRun() throws InterruptedException {
		log.trace("Entering testFullRun()...");
		final Set<Resource> successfulUris = testMain.fullRun(transformation, uris);
		final long startTime = currentTimeMillis();
		synchronized (testSink) {
			while (testSink.accepted().size() < successfulUris.size() && currentTimeMillis() < (startTime + TIMEOUT)) {
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
		log.trace("Leaving testFullRun().");
	}

	@Test
	public void testRunWithUnloadableResources() throws InterruptedException {
		log.trace("Entering testRunWithUnloadableResources()...");
		final Set<Resource> urisWithExtra = new HashSet<>(uris);
		final Set<Resource> badUris = singleton(createResource());
		urisWithExtra.addAll(badUris);
		final Set<Resource> successfulUris = testMain.fullRun(transformation, urisWithExtra);
		final long startTime = currentTimeMillis();
		synchronized (testSink) {
			while ((testSink.accepted().size() < successfulUris.size()) && currentTimeMillis() < (startTime + TIMEOUT)) {
				testSink.wait(TIMESTEP);
			}
		}
		assertEquals("Didn't find the appropriate resource failing to be retrieved!", badUris,
				difference(urisWithExtra, successfulUris));
		log.trace("Leaving testRunWithUnloadableResources().");
	}

	@Test
	public void testMainMethodExecutes() throws IOException {
		log.trace("Entering testMainMethodExecutes()...");
		final String[] argsWithSeparator = concat(createBasicArgsForMainMethodTest(), new String[] { "-s", "\n" },
				String.class);
		final Exception e = testMainMethod(argsWithSeparator);
		if (e != null) {
			fail("Failed to execute Workflow.main with exception: " + e);
		}
		log.trace("Leaving testMainMethodExecutes().");
	}

	@Test
	public void testMainMethodExecutesWithCustomThreading() throws IOException {
		log.trace("Entering testMainMethodExecutesWithCustomThreading()...");
		final String[] argsWithThreadingParams = concat(createBasicArgsForMainMethodTest(), new String[] {
				"--indexing-threads", "7", "--assembler-threads", "7" }, String.class);
		final Exception e = testMainMethod(argsWithThreadingParams);
		if (e != null) {
			fail("Failed to execute Workflow.main with threading parameters with exception: " + e);
		}
		log.trace("Leaving testMainMethodExecutesWithCustomThreading().");
	}

	@Test
	public void testMainMethodExecutesWithPersistedCache() throws IOException {
		log.trace("Entering testMainMethodExecutesWithPersistedCache()...");
		final String cacheDirectory = createTempDir().getAbsolutePath();
		final String[] argsWithPersistence = concat(createBasicArgsForMainMethodTest(), new String[] { "-c",
				cacheDirectory, "-a", "application/rdf+xml" }, String.class);
		final Exception e = testMainMethod(argsWithPersistence);
		if (e != null) {
			fail("Failed to execute Workflow.main with persisted RDF cache with exception: " + e);
		}
		log.trace("Leaving testMainMethodExecutesWithPersistedCache().");
	}

	@Test
	public void testHelp() {
		final Exception e = testMainMethod(new String[] { "-h" });
		if (e != null) {
			fail("Failed to execute Workflow.main for help message with exception: " + e);
		}
	}

	@Test
	public void testSkipRetrieval() throws IOException {
		final String cacheDirectory = createTempDir().getAbsolutePath();
		final Dataset dataset = createDataset(cacheDirectory);
		final Set<Resource> retrievedResources = new CacheAssembler(dataset).uris(uris).call();
		assertEquals("Failed to cache all resources!", uris, retrievedResources);
		final String[] args = concat(createBasicArgsForMainMethodTest(), new String[] { "-c", cacheDirectory,
				"--skip-retrieval" }, String.class);
		final Exception e = testMainMethod(args);
		if (e != null) {
			fail("Failed to execute Workflow.main with preassembled cache with exception: " + e);
		}
	}

	@Test
	public void testSkipRetrievalWithNoCache() throws IOException {
		final String[] args = concat(createBasicArgsForMainMethodTest(), new String[] { "--skip-retrieval" },
				String.class);
		final Exception e = testMainMethod(args);
		if (e != null) {
			fail("Failed to execute Workflow.main with no cache and no retrieval step with exception: " + e);
		}
	}

	@Test
	public void testMissingRequiredCLIArgs() {
		Exception e = testMainMethod(new String[] { "-t", "fakeTransform", "-u", "fakeUris" });
		if (e == null) {
			fail("Should have failed to execute Workflow.main without required parameter for output directory!");
		}
		e = testMainMethod(new String[] { "-t", "fakeTransform", "-o", "fakeOutputDirectory" });
		if (e == null) {
			fail("Should have failed to execute Workflow.main without required parameter for input URIs!");
		}
		e = testMainMethod(new String[] { "-u", "fakeUris", "-o", "fakeOutputDirectory" });
		if (e == null) {
			fail("Should have failed to execute Workflow.main without required parameter for indexing transform!");
		}
		e = testMainMethod(new String[] { "-o", "fakeOutputDirectory" });
		if (e == null) {
			fail("Should have failed to execute Workflow.main without required parameters for indexing transform and input URIs!");
		}
		e = testMainMethod(new String[] { "-u", "fakeUris" });
		if (e == null) {
			fail("Should have failed to execute Workflow.main without required parameters for indexing transform and output directory!");
		}
		e = testMainMethod(new String[] { "-t", "fakeTransform" });
		if (e == null) {
			fail("Should have failed to execute Workflow.main without required parameters for input URIs and output directory!");
		}
		e = testMainMethod(new String[] {});
		if (e == null) {
			fail("Should have failed to execute Workflow.main without required parameters for input URIs, indexing transform, and output directory!");
		}
	}

	private String[] createBasicArgsForMainMethodTest() throws IOException {
		final String outputDir = createTempDir().getAbsolutePath();
		final File transformFile = createTempFile("transform" + randomUUID(), "");
		write(transformation, transformFile, UTF_8);
		final File urisFile = createTempFile("uris" + randomUUID(), "");
		write(on('\n').join(uris), urisFile, UTF_8);
		return new String[] { "-o", outputDir, "-t", transformFile.getAbsolutePath(), "-u", urisFile.getAbsolutePath() };
	}

	private Exception testMainMethod(final String args[]) {
		try {
			new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						main(args);
					} catch (final Exception e) {
						propagate(e);
					}

				}
			}).run();
		} catch (final Exception e) {
			return e;
		}
		return null;
	}
}
