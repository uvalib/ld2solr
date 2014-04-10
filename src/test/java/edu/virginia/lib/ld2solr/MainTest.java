package edu.virginia.lib.ld2solr;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

public class MainTest extends TestHelper {

	private Main testMain;

	private static final String transformation = "title = dc:title :: xsd:string;";

	@Test
	public void testFullRun() throws InterruptedException, ExecutionException {
		testMain = new Main();
		testMain.fullRun(transformation, uris);
	}

}
