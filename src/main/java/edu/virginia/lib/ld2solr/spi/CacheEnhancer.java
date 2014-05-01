package edu.virginia.lib.ld2solr.spi;

public interface CacheEnhancer<T extends CacheEnhancer<T, CacheType>, CacheType> {

	/**
	 * Set the cache assembler over which to operate.
	 * 
	 * @param cache
	 *            the {@link CacheAssembler} to use under this
	 *            {@link CacheEnhancer}
	 * @return this {@link CacheEnhancer} for further operation
	 */
	public T cacheAssembler(CacheAssembler<?, CacheType> cache);

	/**
	 * Enhance this cache with more knowledge.
	 * 
	 * @param input
	 *            some useful input for the enhancement process
	 */
	public void enhance();

	/**
	 * Frees any resources associated to this {@link CacheEnhancer}.
	 * 
	 * @throws InterruptedException
	 */
	public void shutdown() throws InterruptedException;

	public static class NoOp<C> implements CacheEnhancer<NoOp<C>, C> {

		@Override
		public NoOp<C> cacheAssembler(final CacheAssembler<?, C> cache) {
			return this;
		}

		@Override
		public void enhance() {
		}

		@Override
		public void shutdown() {
		}
	}
}
