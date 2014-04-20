package edu.virginia.lib.ld2solr.impl;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;

import com.google.common.util.concurrent.ListeningExecutorService;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.api.OutputRecord;
import edu.virginia.lib.ld2solr.spi.OutputStage;

public class TestOutputStage implements OutputStage<TestOutputStage> {

	private Acceptor<OutputRecord, ?> nextStage;

	private final ListeningExecutorService threadpool = sameThreadExecutor();

	@Override
	public void accept(final NamedFields fields) {
		nextStage.accept(new OutputRecord() {

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

	@Override
	public void andThen(final Acceptor<OutputRecord, ?> a) {
		this.nextStage = a;

	}

	@Override
	public ListeningExecutorService threadpool() {
		return threadpool;
	}

}
