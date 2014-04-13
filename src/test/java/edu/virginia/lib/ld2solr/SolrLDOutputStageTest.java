package edu.virginia.lib.ld2solr;

import static edu.virginia.lib.ld2solr.api.NamedFields.ID_FIELD;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import edu.virginia.lib.ld2solr.TestAcceptor.TestSink;
import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.api.OutputRecord;

public class SolrLDOutputStageTest {

	private SolrLDOutputStage testStage;

	private TestSink testSink;

	private static final String testFieldValue = "testFieldValue";

	private NamedFields fields;

	private static final Logger log = getLogger(SolrLDOutputStageTest.class);

	@Before
	public void setUp() {
		testStage = new SolrLDOutputStage();
		testSink = new TestSink();
		testStage.andThen(testSink);
	}

	/**
	 * 
	 */
	@Test
	public void testAccept() {
		fields = new NamedFields(ImmutableMap.<String, Collection<?>> of(ID_FIELD, asList(testFieldValue)));
		testStage.accept(fields);
		assertTrue("Didn't find our record in output from test stage!",
				Iterables.any(testSink.accepted(), new Predicate<OutputRecord>() {

					@Override
					public boolean apply(final OutputRecord record) {

						return record.id().equals(fields.id());
					}
				}));
		final String outputBytes = new String(testSink.accepted().iterator().next().record());
		log.debug("Retrieved record: {}", outputBytes);
		assertTrue("Failed to find test value in output record!", outputBytes.contains(testFieldValue));
	}
}
