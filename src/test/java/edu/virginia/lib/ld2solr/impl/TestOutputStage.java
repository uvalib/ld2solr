package edu.virginia.lib.ld2solr.impl;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.api.OutputRecord;
import edu.virginia.lib.ld2solr.spi.AbstractStage;
import edu.virginia.lib.ld2solr.spi.OutputStage;

public class TestOutputStage extends AbstractStage<OutputRecord> implements OutputStage {

	@Override
	public void accept(final NamedFields fields) {
		next(new OutputRecord() {

			private final String id = fields.id();

			@Override
			public String id() {
				return id;
			}

			@Override
			public byte[] record() {
				return fields.toString().getBytes();
			}

			@Override
			public String toString() {
				return "Record id: " + id;
			}
		});
	}
}
