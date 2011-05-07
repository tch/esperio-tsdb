package info.chodakowski.opentsdb;

import info.chodakowski.esper.esperio.opentsdb.Pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBInterface;
import net.opentsdb.core.WritableDataPoints;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.hbase.async.HBaseClient;

//TODO;tch: eventually settle with only one hbase client
public class OpenTSDBUtils {
	public static final String OTSDB_DEFAULT_TS_TABLE_NAME = "tsdb";
	public static final String OTSDB_DEFAULT_UID_TABLE_NAME = "tsdb-uid";
	
	public static void purgeAllData(final String path) throws IOException {

		Configuration config = HBaseConfiguration.create();

	    // This instantiates an HTable object that connects you to
	    // the "myLittleHBaseTable" table.
	    HTable tsdbTable = new HTable(config, OTSDB_DEFAULT_TS_TABLE_NAME);
	    purgeTable(tsdbTable);
	    tsdbTable.close();
	    
	    //TODO;tch: do not remove timeseries ids at the moment, this will require more selective treatment
	    //HTable tsdbuidTable = new HTable(config, OTSDB_DEFAULT_UID_TABLE_NAME);
	    //purgeTable(tsdbuidTable);
	    //tsdbuidTable.close();

	}
	
	public static void purgeTable(final HTable table) throws IOException {
		
	    Scan s = new Scan();
	    //s.addFamily(Bytes.toBytes("t"));
	    
	    ResultScanner scanner = table.getScanner(s);

        for(Result rr : scanner) {
	      Delete d = new Delete(rr.getRow());
	      table.delete(d);
	    }
		
	}
	
	public static void addDataPoints(final String metric,
			                         final Pair<String, String> tag,
			                         final Pair<Long, Long>... points) throws Exception {
		
	    HBaseClient hbclient = new HBaseClient("localhost");
	    TSDBInterface tsdb = new TSDB(hbclient, OTSDB_DEFAULT_TS_TABLE_NAME, OTSDB_DEFAULT_UID_TABLE_NAME);
	    
		final WritableDataPoints dps = tsdb.newDataPoints();
		
		Map<String, String> tags = new HashMap<String, String>(1);
		tags.put(tag.getFirst(), tag.getSecond());
		
	    dps.setSeries(metric, tags);
	    
		//Deferred<Object> d;
	    for( Pair<Long, Long> d : points) {
	    	dps.addPoint(d.getFirst(), d.getSecond());
	    }
        
        tsdb.shutdown().joinUninterruptibly();
	}

}
