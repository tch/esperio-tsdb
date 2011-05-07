package info.chodakowski.esper.esperio.opentsdb;

import info.chodakowski.opentsdb.OpenTSDBUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.hbase.async.HBaseClient;

import net.opentsdb.core.Aggregators;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBInterface;


public class SimpleOpenTSDBTimeSeriesScanner implements TimeSeriesScanner<DataPoint>
{
	private HBaseClient hbClient;
	private TSDBInterface tsdb;
	private String metric;
	private Map<String, String> tags = new HashMap<String, String>(8);
	private DataPoints dataPoints;
	private long startTime;
	private long endTime;
	
//	//opentsdb://localhost/metric/?start=234;end=12341;
//	public SimpleOpenTSDBTimeSeriesScanner(final URI uri) {
//	    validateUri(uri);
//		
//        HBaseClient hbclient = new HBaseClient("localhost");
//	    tsdb = new TSDB(hbclient, "tsdb", "tsdb-uid");
//	}
	
//	private void validateUri(final URI uri) {
//		if(uri==null) 
//			throw new IllegalArgumentException("URI cannot be null");
//	}

	public SimpleOpenTSDBTimeSeriesScanner(final String host, final String metric,
			                         final long startTime, final long endTime,
			                         final Pair<String, String>... tags ) {
		this.metric = metric;
		for(Pair<String, String> tag : tags) {
			this.tags.put(tag.getFirst(), tag.getSecond());
		}
		
		this.startTime = startTime;
		this.endTime = endTime;
		
		this.hbClient = new HBaseClient(host);
		this.tsdb = new TSDB(hbClient, OpenTSDBUtils.OTSDB_DEFAULT_TS_TABLE_NAME,
				                       OpenTSDBUtils.OTSDB_DEFAULT_UID_TABLE_NAME);
    }

	/* (non-Javadoc)
	 * @see info.chodakowski.esper.esperio.opentsdb.TimeSeriesScanner#close()
	 */
	@Override
	public void close() throws IOException {
		try {
			tsdb.flush();
			tsdb.shutdown().joinUninterruptibly();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see info.chodakowski.esper.esperio.opentsdb.TimeSeriesScanner#iterator()
	 */
	@Override
	public Iterator<DataPoint> iterator() {

        Query q = tsdb.newQuery();
        q.setTimeSeries(metric, tags, Aggregators.SUM, false);
        q.setStartTime(startTime);  q.setEndTime(endTime);
        //no group_bys for now
        dataPoints = q.run()[0];

		return dataPoints.iterator();
	}
		
}
