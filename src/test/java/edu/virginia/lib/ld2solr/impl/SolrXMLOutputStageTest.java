package edu.virginia.lib.ld2solr.impl;

import static edu.virginia.lib.ld2solr.api.NamedFields.ID_FIELD;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Collection;

import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.api.OutputRecord;
import edu.virginia.lib.ld2solr.impl.TestAcceptor.TestSink;

public class SolrXMLOutputStageTest {

	private SolrXMLOutputStage testStage;

	private TestSink testSink;

	private static final String testFieldValue = "testFieldValue";

	private NamedFields fields;

	private static final Logger log = getLogger(SolrXMLOutputStageTest.class);

	@Before
	public void setUp() {
		testStage = new SolrXMLOutputStage();
		testSink = new TestSink();
		testStage.andThen(testSink);
		fields = new NamedFields(ImmutableMap.<String, Collection<?>> of(ID_FIELD, asList(testFieldValue)));
	}

	/**
	 * 
	 */
	@Test
	public void testAccept() {
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

	@Test
	public void testWrap() {
		testStage = new SolrXMLOutputStage(1);
		final SolrInputDocument result = SolrXMLOutputStage.wrap.apply(fields);
		assertTrue("Should have found test field value under test field name in Solr document!",
				result.getFieldValues(ID_FIELD).contains(testFieldValue));

	}
}
