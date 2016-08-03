package org.bimserver.database.berkeley;


/******************************************************************************
 * Copyright (C) 2009-2016  BIMserver.org
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see {@literal<http://www.gnu.org/licenses/>}.
 *****************************************************************************/

import org.bimserver.database.BimserverLockConflictException;
import org.bimserver.database.Record;
import org.bimserver.database.RecordIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;
import java.lang.Exception;
import java.util.NoSuchElementException;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;


public class BerkeleyRecordIterator implements RecordIterator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(BerkeleyRecordIterator.class);
	private BerkeleyKeyValueStore berkeleyKeyValueStore;
	private static Session session;
	String key;
	TableWrapper value ;
	
	public BerkeleyRecordIterator( TableWrapper tableName, BerkeleyKeyValueStore berkeleyKeyValueStore) {
		
		this.value = tableName ;
		this.berkeleyKeyValueStore = berkeleyKeyValueStore;
	}

	public Record next(){
		
		try {
			
			ResultSet rs = session.execute(key,value);
			Iterator<Row> iter = rs.iterator();
			
			if (iter.hasNext()) {
				
				return new BerkeleyRecord(key, value);
			} else {
				return null;
			}
		} catch(NoSuchElementException s)
				{
					LOGGER.error("" , s.getMessage());
				}
		return null;
	}

	@Override
	public void close() {
		try {
				session.close();
			berkeleyKeyValueStore.close();
		} catch (Exception e) {
			LOGGER.error("", e);
		}
	}

	@Override
	public Record last() throws BimserverLockConflictException {
		
		try {
			ResultSet rs = session.execute(key,value);
			Iterator<Row> iter = rs.iterator();
			if (iter.hasNext()) {
				return new BerkeleyRecord(key, value);
			} else {
				return null;
			}
		} catch(NoSuchElementException s)
		{
			LOGGER.error("" , s.getMessage());
		}
		return null;
	}
}