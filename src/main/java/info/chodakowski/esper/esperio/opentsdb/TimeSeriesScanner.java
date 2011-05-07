package info.chodakowski.esper.esperio.opentsdb;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;


public interface TimeSeriesScanner<T> extends Iterable<T>, Closeable {

	public void close() throws IOException;

	public Iterator<T> iterator();

}