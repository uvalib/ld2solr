package edu.virginia.lib.ld2solr;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

public class MainTest extends TestHelper {

	private Main testMain;

	private static final String transformation = "title = dc:title :: xsd:string;";

	@Test
	public void testFullRun() throws InterruptedException, IOException, ExecutionException {
		testMain = new Main();
		testMain.fullRun(transformation, uris);
	}

}
