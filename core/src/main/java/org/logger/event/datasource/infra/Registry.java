package org.logger.event.datasource.infra;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

public class Registry implements Iterable<Register> {

	private List<Register> registers = null;
	private final Iterator<Register> internalIterator;

	@Override
	public Iterator<Register> iterator() {
		return new Iterator<Register>() {

			@Override
			public boolean hasNext() {
				return internalIterator.hasNext();
			}

			@Override
			public Register next() {
				return internalIterator.next();
			}

			@Override
			public void forEachRemaining(Consumer<? super Register> arg0) {

			}

			@Override
			public void remove() {

			}

		};
	}

	public Registry() {
		registers = new ArrayList<Register>();
		registers.add(CassandraClient.instance());
		/**
		 * To be Enable 
		registers.add(ELSClient.instance());
		*/
		internalIterator = registers.iterator();
	}

	@Override
	public void forEach(Consumer<? super Register> arg0) {
	}

	@Override
	public Spliterator<Register> spliterator() {
		return null;
	}
}
