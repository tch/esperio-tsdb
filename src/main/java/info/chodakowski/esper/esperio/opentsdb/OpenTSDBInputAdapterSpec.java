/**************************************************************************************
 * Copyright (C) 2008 EsperTech, Inc. All rights reserved.                            *
 * http://esper.codehaus.org                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package info.chodakowski.esper.esperio.opentsdb;

import java.util.Map;

/**
 * A spec for OpenTSDBAdapter.
 */
public class OpenTSDBInputAdapterSpec
{
	
	private String location; //location of the opentsdb database, ex. "localhost"

	private String metric;
	private Pair<String, String> tag;

	private long startTime = 1200; //TODO;tch; bug in OpenTSDB: https://github.com/stumbleupon/opentsdb/issues/46
	private long endTime = Long.MAX_VALUE;
	
	private String eventTypeName;

	private String[] propertyOrder;
	private Map<String, Object> propertyTypes;

	private Integer eventsPerSec;
	//private boolean looping;
	
	private boolean usingEngineThread, usingExternalTimer;
	

	/**
	 * Ctor.
	 * @param eventTypeName - the name for the event type created from the data
	 */
	public OpenTSDBInputAdapterSpec(final String location, final String metric, final Pair<String, String> tag, final String eventTypeName)
	{
		this.location = location;
		this.metric = metric;
		this.tag = tag;
		this.eventTypeName = eventTypeName;
	}

	/**
	 * @return the location
	 */
	public String getLocation() {
		return location;
	}

	/**
	 * @return the metric
	 */
	public String getMetric() {
		return metric;
	}

	/**
	 * @return the tag
	 */
	public Pair<String, String> getTag() {
		return tag;
	}

	/**
	 * @return the startTime
	 */
	public long getStartTime() {
		return startTime;
	}

	
	/**
	 * @return the endTime
	 */
	public long getEndTime() {
		return endTime;
	}


	/**
	 * @return the usingEngineThread
	 */
	public boolean isUsingEngineThread()
	{
		return usingEngineThread;
	}

    /**
     * @return true for using external timer
     */
	public boolean isUsingExternalTimer()
	{
		return usingExternalTimer;
	}

	/**
	 * @return the eventTypeName
	 */
	public String getEventTypeName()
	{
		return eventTypeName;
	}


	/**
	 * @return the eventsPerSec
	 */
	public Integer getEventsPerSec()
	{
		return eventsPerSec;
	}

	/**
	 * @return the looping
	 */
//	public boolean isLooping()
//	{
//		return looping;
//	}

	/**
	 * @return the propertyOrder
	 */
	public String[] getPropertyOrder()
	{
		return propertyOrder;
	}

	/**
	 * @return the propertyTypes
	 */
	public Map<String, Object> getPropertyTypes()
	{
		return propertyTypes;
	}

}
