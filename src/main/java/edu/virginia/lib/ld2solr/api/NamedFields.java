package edu.virginia.lib.ld2solr.api;

import static com.google.common.collect.Collections2.transform;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;

/**
 * A {@link Map} of named fields. Any field may be multivalued.
 * 
 * @author ajs6f
 * 
 */
public class NamedFields implements Map<String, Collection<String>> {

	Map<String, Collection<?>> fields;

	public NamedFields(final Map<String, Collection<?>> wrappedFields) {
		this.fields = wrappedFields;
	}

	@Override
	public int size() {
		return fields.size();
	}

	@Override
	public boolean isEmpty() {
		return fields.isEmpty();
	}

	@Override
	public boolean containsKey(final Object key) {
		return fields.containsKey(key);
	}

	@Override
	public boolean containsValue(final Object value) {
		return fields.containsValue(value);
	}

	@Override
	public Collection<String> get(final Object key) {
		return transform(fields.get(key), asString);
	}

	@Override
	public Collection<String> put(final String key, final Collection<String> value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<String> remove(final Object key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void putAll(final Map<? extends String, ? extends Collection<String>> m) {
		throw new UnsupportedOperationException();

	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();

	}

	@Override
	public Set<String> keySet() {
		return fields.keySet();
	}

	@Override
	public Collection<Collection<String>> values() {
		return transform(fields.values(), asCollectionOfStrings);
	}

	@Override
	public Set<java.util.Map.Entry<String, Collection<String>>> entrySet() {
		return Maps.transformEntries(fields, asEntryOfStringToCollectionOfStrings).entrySet();
	}

	private static final Function<Object, String> asString = new Function<Object, String>() {

		@Override
		public String apply(final Object input) {
			return input.toString();
		}
	};

	private static final Function<Collection<?>, Collection<String>> asCollectionOfStrings = new Function<Collection<?>, Collection<String>>() {

		@Override
		public Collection<String> apply(final Collection<?> input) {
			return transform(input, asString);
		}
	};

	private static final EntryTransformer<String, Collection<?>, Collection<String>> asEntryOfStringToCollectionOfStrings = new EntryTransformer<String, Collection<?>, Collection<String>>() {

		@Override
		public Collection<String> transformEntry(final String key, final Collection<?> value) {

			return asCollectionOfStrings.apply(value);
		}
	};

}
