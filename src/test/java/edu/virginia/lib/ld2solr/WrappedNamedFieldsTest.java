package edu.virginia.lib.ld2solr;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import edu.virginia.lib.ld2solr.api.NamedFields;

public class WrappedNamedFieldsTest {

	private SolrWrappedNamedFields testWrappedNamedFields;

	private static final String testFieldName = "testFieldName";

	private static final String testFieldValue = "testFieldValue";

	private static final List<String> testFieldValues = asList(testFieldValue);

	private static NamedFields mockNamedFields = new NamedFields(ImmutableMap.<String, Collection<?>> of(testFieldName,
			testFieldValues));

	@Before
	public void setUp() {
		testWrappedNamedFields = new SolrWrappedNamedFields(mockNamedFields);
	}

	@Test
	public void testGet() {
		final SolrInputDocument result = testWrappedNamedFields.get();
		assertTrue("Should have found test field name in Solr document!", result.containsKey(testFieldName));
		assertTrue("Should have found test field value under test field name in Solr document!",
				result.getFieldValues(testFieldName).contains(testFieldValue));

	}
}
