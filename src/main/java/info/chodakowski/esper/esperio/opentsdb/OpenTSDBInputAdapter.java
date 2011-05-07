/**************************************************************************************
 * Copyright (C) 2008 EsperTech, Inc. All rights reserved.                            *
 * http://esper.codehaus.org                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package info.chodakowski.esper.esperio.opentsdb;

import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.core.EPServiceProviderSPI;
import com.espertech.esper.event.EventAdapterService;
import com.espertech.esper.event.map.MapEventType;
import com.espertech.esper.client.EventType;
import com.espertech.esper.client.PropertyAccessException;
import com.espertech.esperio.*;
import com.espertech.esper.util.JavaClassHelper;
import com.espertech.esper.util.ExecutionPathDebugLog;
import com.espertech.esper.adapter.InputAdapter;
import com.espertech.esper.adapter.AdapterState;

import net.opentsdb.core.DataPoint;
import net.sf.cglib.core.ReflectUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.*;
import java.beans.PropertyDescriptor;

/**
 * An event Adapter that uses a CSV file for a source.
 */
public class OpenTSDBInputAdapter extends AbstractCoordinatedAdapter implements InputAdapter
{
	private static final Logger logger = LoggerFactory.getLogger(OpenTSDBInputAdapter.class);

	private OpenTSDBInputAdapterSpec adapterSpec;
	
	private TimeSeriesScanner<DataPoint> scanner;// = new SimpleOpenTSDBTimeSeriesScanner();
	private Iterator<DataPoint> iterator;
	
	private Integer eventsPerSec;
	private AbstractTypeCoercer coercer = new BasicTypeCoercer();
	private String[] propertyOrder;
	private Map<String, Object> propertyTypes;
	private String eventTypeName;
	private long lastTimestamp = 0;
	private long totalDelay;
	boolean atEOF = false;
	
    private Class beanClass;
    
    //private String[] firstRow;
	private int rowCount = 0;

    /**
	 * Ctor.
	 * @param epService - provides the engine runtime and services
	 * @param spec - the parameters for this adapter
	 */
	public OpenTSDBInputAdapter(final EPServiceProvider epService, final OpenTSDBInputAdapterSpec spec)
	{
		super(epService, spec.isUsingEngineThread(), spec.isUsingExternalTimer());

		adapterSpec = spec;
		eventsPerSec = spec.getEventsPerSec();

		if(epService != null)
		{
			finishInitialization(epService, spec);
		}
	}

	/**
	 * Ctor for adapters that will be passed to an AdapterCoordinator.
	 * @param adapterSpec contains parameters that specify the behavior of the input adapter
	 */
	public OpenTSDBInputAdapter(final OpenTSDBInputAdapterSpec adapterSpec)
	{
		this(null, adapterSpec);
	}

	/* (non-Javadoc)
	 * @see com.espertech.esperio.ReadableAdapter#read()
	 */
	public SendableEvent read() throws EPException
	{
		if(stateManager.getState() == AdapterState.DESTROYED || atEOF)
		{
			return null;
		}

		try
		{
			if(eventsToSend.isEmpty())
			{
                if (beanClass != null)
                {
                     return new SendableBeanEvent(newMapEvent(), beanClass, eventTypeName, totalDelay, scheduleSlot);
                }
                else
                {
                    return new SendableMapEvent(newMapEvent(), eventTypeName, totalDelay, scheduleSlot);
                }
            }
			else
			{
				SendableEvent event = eventsToSend.first();
				eventsToSend.remove(event);
				return event;
			}
		}
		catch (EOFException e)
		{
            if ((ExecutionPathDebugLog.isDebugEnabled) && (logger.isDebugEnabled()))
            {
			    logger.debug(".read reached end of the event stream");
            }
            atEOF = true;
			if(stateManager.getState() == AdapterState.STARTED)
			{
				stop();
			}
			else
			{
				destroy();
			}
			return null;
		}
	}



	/* (non-Javadoc)
	 * @see com.espertech.esperio.AbstractCoordinatedAdapter#setEPService(com.espertech.esper.client.EPServiceProvider)
	 */
	@Override
	public void setEPService(EPServiceProvider epService)
	{
		super.setEPService(epService);
		finishInitialization(epService, adapterSpec);
	}

    /**
     * Sets the coercing provider.
     * @param coercer to use for coercing
     */
    public void setCoercer(AbstractTypeCoercer coercer) {
		this.coercer = coercer;
	}

	/**
	 * Close the adapter.
	 */
	protected void close()
	{
		try {
			scanner.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Remove the first member of eventsToSend. If there is
	 * another record in the CSV file, insert the event created
	 * from it into eventsToSend.
	 */
	protected void replaceFirstEventToSend()
	{
		eventsToSend.remove(eventsToSend.first());
		SendableEvent event = read();
		if(event != null)
		{
			eventsToSend.add(event);
		}
	}

	/**
	 * Reset all the changeable state of this ReadableAdapter, as if it were just created.
	 */
	protected void reset()
	{
//		lastTimestamp = 0;
//		totalDelay = 0;
//		atEOF = false;
//		if(reader.isResettable())
//		{
//			scanner.reset();
//		}
		//throw new OperationNotSupportedException("resetting not supported currently");
	}

	@SuppressWarnings("unchecked")
	private void finishInitialization(EPServiceProvider epService, OpenTSDBInputAdapterSpec spec)
	{
		assertValidParameters(epService, spec);

		EPServiceProviderSPI spi = (EPServiceProviderSPI)epService;

		scheduleSlot = spi.getSchedulingMgmtService().allocateBucket().allocateSlot();

		scanner = new SimpleOpenTSDBTimeSeriesScanner(spec.getLocation(), spec.getMetric(), spec.getStartTime(), spec.getEndTime(),
				                                      spec.getTag());
		iterator = scanner.iterator();
		
		Map<String, Object> givenPropertyTypes = constructPropertyTypes(spec.getEventTypeName(), spec.getPropertyTypes(), spi.getEventAdapterService());

		propertyOrder = spec.getPropertyOrder();

		propertyTypes = resolvePropertyTypes(givenPropertyTypes);

		if(givenPropertyTypes == null)
		{
			spi.getEventAdapterService().addNestableMapType(eventTypeName, new HashMap<String, Object>(propertyTypes), null, true, true, true, false, false);
		}

		coercer.setPropertyTypes(propertyTypes);
	}

	private Map<String, Object> newMapEvent() throws EOFException
	{
		++rowCount;
		DataPoint dp = iterator.next();
		updateTotalDelay(dp, rowCount==1); //TODO;replace with proper "is first row semantics"
		
		Map<String, Object> map = createMapFromDataPoint(dp);
		return map;
	}

	private Map<String, Object> createMapFromDataPoint(final DataPoint dp)
	{
		Map<String, Object> map = new HashMap<String, Object>();

		int count = 0;

		try
		{
			for(String property : propertyOrder)
			{
				// Skip properties that are not
				// part of the map to send
				if ((propertyTypes != null) &&
                    (!propertyTypes.containsKey(property)))
                {
					count++;
					continue;
				}
				Object value = coercer.coerce(property, dp.longValue());
				map.put(property, value);
			}
		}
		catch (Exception e)
		{
			throw new EPException(e);
		}
		return map;
	}

	private Map<String, Object> constructPropertyTypes(String eventTypeName, Map<String, Object> propertyTypesGiven, EventAdapterService eventAdapterService)
	{
		Map<String, Object> propertyTypes = new HashMap<String, Object>();
		EventType eventType = eventAdapterService.getExistsTypeByName(eventTypeName);
		if(eventType == null)
		{
			if(propertyTypesGiven != null)
			{
				eventAdapterService.addNestableMapType(eventTypeName, new HashMap<String, Object>(propertyTypesGiven), null, true, true, true, false, false);
			}
			return propertyTypesGiven;
		}
		if(!eventType.getUnderlyingType().equals(Map.class))
		{
            beanClass = eventType.getUnderlyingType();
		}
		if(propertyTypesGiven != null && eventType.getPropertyNames().length != propertyTypesGiven.size())
		{
			// allow this scenario for beans as we may want to bring in a subset of properties
			if (beanClass != null) {
				return propertyTypesGiven;
			}
			else {
				throw new EPException("Event type " + eventTypeName + " has already been declared with a different number of parameters");
			}
		}
		for(String property : eventType.getPropertyNames())
		{
            Class type;
            try {
                type = eventType.getPropertyType(property);
            }
            catch (PropertyAccessException e) {
                // thrown if trying to access an invalid property on an EventBean
                throw new EPException(e);
            }
			if(propertyTypesGiven != null && propertyTypesGiven.get(property) == null)
			{
				throw new EPException("Event type " + eventTypeName + "has already been declared with different parameters");
			}
			if(propertyTypesGiven != null && !propertyTypesGiven.get(property).equals(type))
			{
				throw new EPException("Event type " + eventTypeName + "has already been declared with a different type for property " + property);
			}
            // we can't set read-only properties for bean
            if(!eventType.getUnderlyingType().equals(Map.class)) {
            	PropertyDescriptor[] pds = ReflectUtils.getBeanProperties(beanClass);
            	PropertyDescriptor pd = null;
            	for (PropertyDescriptor p :pds) {
            		if (p.getName().equals(property))
            			pd = p;
            	}
                if (pd == null)
                {
                    continue;
                }
                if (pd.getWriteMethod() == null) {
            		if (propertyTypesGiven == null) {
            			continue;
            		}
            		else {
            			throw new EPException("Event type " + eventTypeName + "property " + property + " is read only");
            		}
            	}
            }
			propertyTypes.put(property, type);
		}
		
		// flatten nested types
		Map<String, Object> flattenPropertyTypes = new HashMap<String, Object>();
		for (String p : propertyTypes.keySet()) {
			Object type = propertyTypes.get(p);
			if (type instanceof Class && ((Class)type).getName().equals("java.util.Map") && eventType instanceof MapEventType) {
				MapEventType mapEventType = (MapEventType) eventType;
				Map<String, Object> nested = (Map) mapEventType.getTypes().get(p);
				for (String nestedProperty : nested.keySet()) {
					flattenPropertyTypes.put(p+"."+nestedProperty, nested.get(nestedProperty));
				}
			} else if (type instanceof Class) {
				Class c = (Class)type;
				if (!c.isPrimitive() && !c.getName().startsWith("java")) {
					PropertyDescriptor[] pds = ReflectUtils.getBeanProperties(c);
					for (PropertyDescriptor pd : pds) {
						if (pd.getWriteMethod()!=null)
							flattenPropertyTypes.put(p+"."+pd.getName(), pd.getPropertyType());
					}
				} else {
					flattenPropertyTypes.put(p, type);
				}
			} else {
				flattenPropertyTypes.put(p, type);
			}
		}
		return flattenPropertyTypes;
	}

	private void updateTotalDelay(final DataPoint dp, boolean isFirstRow)
	{
		if(eventsPerSec != null)
		{
			int msecPerEvent = 1000/eventsPerSec;
			totalDelay += msecPerEvent;
		}
		else
		{
			final long timestamp = dp.timestamp();
			
			if(timestamp < 0)
			{
				throw new EPException("Encountered negative timestamp for an event : " + timestamp);
			}
			else
			{
				long timestampDifference = 0;
				if(timestamp < lastTimestamp)
				{
					if(!isFirstRow)
					{
						throw new EPException("Subsequent timestamp " + timestamp + " is smaller than previous timestamp " + lastTimestamp);
					}
					else
					{
						timestampDifference = timestamp;
					}
				}
				else
				{
					timestampDifference = timestamp - lastTimestamp;
				}
				lastTimestamp = timestamp;
				totalDelay += timestampDifference;
			}
		}
	}

	private Map<String, Object> resolvePropertyTypes(Map<String, Object> propertyTypes)
	{
		if(propertyTypes != null)
		{
			return propertyTypes;
		}

		Map<String, Object> result = new HashMap<String, Object>();
		for(int i = 0; i < propertyOrder.length; i++)
		{
            String name = propertyOrder[i];
            Class type = String.class;
            if (name.contains(" ")) {
                String[] typeAndName = name.split("\\s");
                try {
                    name = typeAndName[1];
                    type = JavaClassHelper.getClassForName(JavaClassHelper.getBoxedClassName(typeAndName[0]));
                    propertyOrder[i] = name;
                } catch (Throwable e) {
                    logger.warn("Unable to use given type for property, will default to String: " + propertyOrder[i], e);
                }
            }
            result.put(name, type);
        }
		return result;
	}

//	private boolean isUsingTitleRow(String[] firstRow, String[] propertyOrder)
//	{
//		if(firstRow == null)
//		{
//			return false;
//		}
//		Set<String> firstRowSet = new HashSet<String>(Arrays.asList(firstRow));
//		Set<String> propertyOrderSet = new HashSet<String>(Arrays.asList(propertyOrder));
//		return firstRowSet.equals(propertyOrderSet);
//	}

//	private String[] getFirstRow()
//	{
//		String[] firstRow;
//		try
//		{
//			firstRow = reader.getNextRecord();
//		}
//		catch (EOFException e)
//		{
//			atEOF = true;
//			firstRow = null;
//		}
//		return firstRow;
//	}

	private void assertValidEventsPerSec(Integer eventsPerSec)
	{
		if(eventsPerSec != null)
		{
			if(eventsPerSec < 1 || eventsPerSec > 1000)
			{
				throw new IllegalArgumentException("Illegal value of eventsPerSec:" + eventsPerSec);
			}
		}
	}

	private void assertValidParameters(EPServiceProvider epService, OpenTSDBInputAdapterSpec adapterSpec)
	{
		if(!(epService instanceof EPServiceProviderSPI))
		{
			throw new IllegalArgumentException("Invalid type of EPServiceProvider");
		}

		if(adapterSpec.getEventTypeName() == null)
		{
			throw new NullPointerException("eventTypeName cannot be null");
		}

		assertValidEventsPerSec(adapterSpec.getEventsPerSec());

//		if(adapterSpec.isLooping() && !adapterSpec.getAdapterInputSource().isResettable())
//		{
//			throw new EPException("Cannot loop on a non-resettable input source");
//		}
	}

    /**
     * Returns row count.
     * @return row count
     */
    public int getRowCount() {
		return rowCount;
	}
}
