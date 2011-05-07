package info.chodakowski.esper.esperio.opentsdb;

import info.chodakowski.opentsdb.OpenTSDBUtils;
import net.opentsdb.core.DataPoint;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

import static info.chodakowski.esper.esperio.opentsdb.Pair.pairOfStrings;


public class SimpleOpenTSDBTimeSeriesScannerTest {
	
	private final static Logger logger = LoggerFactory.getLogger(SimpleOpenTSDBTimeSeriesScanner.class);
	
	private TimeSeriesScanner<DataPoint> underTest;
	
	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		OpenTSDBUtils.purgeAllData("");
		OpenTSDBUtils.addDataPoints("test_metric_01", pairOfStrings("type", "test_type_01"),
				                    new Pair<Long, Long>(1201001l, 1201001l)
				                   ,new Pair<Long, Long>(1202002l, 1202002l)
				                   ,new Pair<Long, Long>(1203003l, 1203003l));
		
		OpenTSDBUtils.addDataPoints("test_metric_01", pairOfStrings("type", "test_type_02"),
                new Pair<Long, Long>(1201001l, 1l)
               ,new Pair<Long, Long>(1202002l, 2l)
               ,new Pair<Long, Long>(1203003l, 3l));

		OpenTSDBUtils.addDataPoints("test_metric_01", pairOfStrings("kind", "test_kind_01"),
                new Pair<Long, Long>(1201001l, 10l)
               ,new Pair<Long, Long>(1202002l, 20l)
               ,new Pair<Long, Long>(1203003l, 30l));
		
		
		this.underTest = new SimpleOpenTSDBTimeSeriesScanner("localhost", "test_metric_01", 1200, 10000000
															,pairOfStrings("type","test_type_01")
															//,pairOfStrings("type","*") //return all rows, and "groups" them into 2 spans but does not exectue the aggregator
															//,pairOfStrings("type","test_type_02") //overwrites previous one
															//,pairOfStrings("kind","test_kind_01") //TODO;tch: fix this - doesn't work with additional tag
															 );
	}

	@After
	public void tearDown() throws Exception {
		underTest.close();
	}

	@Test
	public void testIterator() {
		DataPoint prev = null;
		
		for(DataPoint dp : underTest) {
			
			logger.debug("Got datapoint {}", dp);
			System.out.println("Got datapoint "+dp);
			
			long timestamp = dp.timestamp();
			long value = dp.longValue();
			System.out.println(timestamp+"  "+value);
			
			if(prev!=null)
				assertTrue(dp.timestamp() >= prev.timestamp());
			prev=dp;
		}
	}

}
