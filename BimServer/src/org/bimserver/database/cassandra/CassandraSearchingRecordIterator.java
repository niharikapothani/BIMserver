package org.bimserver.database.cassandra;

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

import java.util.Arrays;
import java.util.Iterator;

import org.bimserver.database.BimserverLockConflictException;
import org.bimserver.database.Record;
import org.bimserver.database.SearchingRecordIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.utils.Bytes;

public class CassandraSearchingRecordIterator implements SearchingRecordIterator {

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSearchingRecordIterator.class);
	private Session session;
	private final byte[] mustStartWith;
	
	private byte[] nextStartSearchingAt;
	
	TableWrapper value ;
	
	private CassandraKeyValueStore CassandraKeyValueStore;

	public CassandraSearchingRecordIterator( CassandraKeyValueStore CassandraKeyValueStore, TableWrapper tableName,byte[] mustStartWith, byte[] startSearchingAt) throws BimserverLockConflictException {
		
		this.CassandraKeyValueStore = CassandraKeyValueStore;
		this.mustStartWith = mustStartWith;
	}

	
	private Record getFirstNext(byte[] startSearchingAt) throws BimserverLockConflictException {
		this.nextStartSearchingAt = null;
		try {
			String key =  Bytes.toHexString(startSearchingAt);
			ResultSet rs = session.execute(key,value);
			Iterator<Row> iter = rs.iterator();
			if (iter.hasNext()) {
				byte[] firstBytes = new byte[mustStartWith.length];
				System.arraycopy(key.getBytes(), 0, firstBytes, 0, mustStartWith.length);
				if (Arrays.equals(firstBytes, mustStartWith)) {
					return new CassandraRecord(key, value);
				}
			}
		} catch (ReadTimeoutException rt) {
		    throw new BimserverLockConflictException(rt);
	    }catch (WriteTimeoutException wt) {
	    	throw new BimserverLockConflictException(wt);
	    } catch (Exception e) {
			LOGGER.error("", e);
		}
		return null;
	}

	@Override
	public Record next() throws BimserverLockConflictException {
		if (nextStartSearchingAt != null) {
			return getFirstNext(nextStartSearchingAt);
		}
		try {
			String key =  Bytes.toHexString(nextStartSearchingAt);
			ResultSet rs = session.execute(key,value);
			Iterator<Row> iter = rs.iterator();
			if (iter.hasNext()) {
				byte[] firstBytes = new byte[mustStartWith.length];
				System.arraycopy(key.getBytes(), 0, firstBytes, 0, mustStartWith.length);
				if (Arrays.equals(firstBytes, mustStartWith)) {
					return new CassandraRecord(key, value);
				}
			}
		} catch (ReadTimeoutException rt) {
		    throw new BimserverLockConflictException(rt);
	    }catch (WriteTimeoutException wt) {
	    throw new BimserverLockConflictException(wt);
	    }catch (Exception e) {
			LOGGER.error("", e);
		}
		return null;
	}

	@Override
	public void close() {
		try {
			session.close();
			CassandraKeyValueStore.close();
		} catch (Exception e) {
			LOGGER.error("", e);
		}
	}

	@Override
	public Record next(byte[] startSearchingAt) throws BimserverLockConflictException {
		return getFirstNext(startSearchingAt);
	}

	@Override
	public Record last() throws BimserverLockConflictException {
		if (nextStartSearchingAt != null) {
			return getFirstNext(nextStartSearchingAt);
		}
		try {
			String key =  Bytes.toHexString(nextStartSearchingAt);
			ResultSet rs = session.execute(key,value);
			Iterator<Row> iter = rs.iterator();
			
			if (iter.hasNext()) {
				byte[] firstBytes = new byte[mustStartWith.length];
				System.arraycopy(key.getBytes(), 0, firstBytes, 0, mustStartWith.length);
				if (Arrays.equals(firstBytes, mustStartWith)) {
					return new CassandraRecord(key, value);
				}
			}
		} catch (ReadTimeoutException rt) {
		    throw new BimserverLockConflictException(rt);
	    }catch (WriteTimeoutException wt) {
	    throw new BimserverLockConflictException(wt);
	    } catch (Exception e) {
			LOGGER.error("", e);
		}
		return null;
	}
}