package edu.virginia.lib.ld2solr;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Joiner.on;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.ObjectArrays.concat;
import static com.google.common.collect.Sets.difference;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.Files.write;
import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static edu.virginia.lib.ld2solr.Main.main;
import static java.io.File.createTempFile;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singleton;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
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
	public void testMainMethodExecutes() throws IOException {
		final String[] argsWithSeparator = concat(createBasicArgsForMainMethodTest(), new String[] { "-s", "\n" },
				String.class);
		final Exception e = testMainMethod(argsWithSeparator);
		if (e != null) {
			fail("Failed to execute Main.main with exception: " + e);
		}
	}

	@Test
	public void testMainMethodExecutesWithPersistedCache() throws IOException {
		final String cacheDirectory = createTempDir().getAbsolutePath();
		final String[] argsWithPersistence = concat(createBasicArgsForMainMethodTest(), new String[] { "-c",
				cacheDirectory }, String.class);
		final Exception e = testMainMethod(argsWithPersistence);
		if (e != null) {
			fail("Failed to execute Main.main with persisted RDF cache with exception: " + e);
		}
	}

	@Test
	public void testHelp() {
		final Exception e = testMainMethod(new String[] { "-h" });
		if (e != null) {
			fail("Failed to execute Main.main for help message with exception: " + e);
		}
	}

	@Test
	public void testMissingRequiredCLIArgs() {
		Exception e = testMainMethod(new String[] { "-t", "fakeTransform", "-u", "fakeUris" });
		if (e == null) {
			fail("Should have failed to execute Main.main without required parameter for output directory!");
		}
		e = testMainMethod(new String[] { "-t", "fakeTransform", "-o", "fakeOutputDirectory" });
		if (e == null) {
			fail("Should have failed to execute Main.main without required parameter for input URIs!");
		}
		e = testMainMethod(new String[] { "-u", "fakeUris", "-o", "fakeOutputDirectory" });
		if (e == null) {
			fail("Should have failed to execute Main.main without required parameter for indexing transform!");
		}
		e = testMainMethod(new String[] { "-o", "fakeOutputDirectory" });
		if (e == null) {
			fail("Should have failed to execute Main.main without required parameters for indexing transform and input URIs!");
		}
		e = testMainMethod(new String[] { "-u", "fakeUris" });
		if (e == null) {
			fail("Should have failed to execute Main.main without required parameters for indexing transform and output directory!");
		}
		e = testMainMethod(new String[] { "-t", "fakeTransform" });
		if (e == null) {
			fail("Should have failed to execute Main.main without required parameters for input URIs and output directory!");
		}
		e = testMainMethod(new String[] {});
		if (e == null) {
			fail("Should have failed to execute Main.main without required parameters for input URIs, indexing transform, and output directory!");
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
