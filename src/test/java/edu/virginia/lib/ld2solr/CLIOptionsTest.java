package edu.virginia.lib.ld2solr;

import static com.google.common.collect.Sets.intersection;
import static edu.virginia.lib.ld2solr.CLIOption.ACCEPT;
import static edu.virginia.lib.ld2solr.CLIOption.helpOptions;
import static edu.virginia.lib.ld2solr.CLIOption.mainOptions;
import static edu.virginia.lib.ld2solr.CLIOption.valueOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;

import org.junit.Test;

public class CLIOptionsTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testHelpAndMainOptionsAreDisjoint() {
		assertTrue("Found either help options in main options or vice versa!",
				intersection(new HashSet<>(mainOptions.getOptions()), new HashSet<>(helpOptions.getOptions()))
						.isEmpty());
	}

	@Test
	public void testValueOf() {
		assertEquals("CLIOption.valueOf() failed to produce the correct instance!", ACCEPT, valueOf("ACCEPT"));
	}
}
