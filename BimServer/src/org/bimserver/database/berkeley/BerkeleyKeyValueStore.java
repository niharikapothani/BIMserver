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

//jdk

import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

//bimserver
import org.bimserver.BimserverDatabaseException;
import org.bimserver.database.BimTransaction;
import org.bimserver.database.BimserverLockConflictException;
import org.bimserver.database.DatabaseSession;
import org.bimserver.database.KeyValueStore;
import org.bimserver.database.Record;
import org.bimserver.database.RecordIterator;
import org.bimserver.database.SearchingRecordIterator;
import org.bimserver.utils.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//cassandra java driver
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.*;

//exceptions
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.exceptions.ConnectionException;

public class BerkeleyKeyValueStore implements KeyValueStore {

	private static final Logger LOGGER = LoggerFactory.getLogger(BerkeleyKeyValueStore.class);
	private static Cluster cluster;
	private static Session session;
	public static final String mykeyspace="keyspace";
	private long committedWrites;
	private long reads;
	private final Map<String, TableWrapper> tables = new HashMap<>();
	private boolean isNew;
	private long lastPrintedReads = 0;
	private long lastPrintedCommittedWrites = 0;
	private boolean useTransactions = true;
	
	public BerkeleyKeyValueStore(Path dataDir) throws DatabaseInitException, NoHostAvailableException
	{
		if (Files.isDirectory(dataDir)) {
			try {
				if (PathUtils.list(dataDir).size() > 0) {
					LOGGER.info("Non-empty database directory found \"" + dataDir.toString() + "\"");
					isNew = false;
				} else {
					LOGGER.info("Empty database directory found \"" + dataDir.toString() + "\"");
					isNew = true;
				}
			} catch (IOException e) {
				LOGGER.error("", e);
			}
		} else {
			isNew = true;
			LOGGER.info("No database directory found, creating \"" + dataDir.toString() + "\"");
			try {
				Files.createDirectory(dataDir);
				LOGGER.info("Successfully created database dir \"" + dataDir.toString() + "\"");
			} catch (Exception e) {
				LOGGER.error("Error creating database dir \"" + dataDir.toString() + "\"");
			}
		}
		PoolingOptions poolingOptions = new PoolingOptions();
		// customize options...
		poolingOptions
	    .setCoreConnectionsPerHost(HostDistance.LOCAL,  4)
	    .setMaxConnectionsPerHost( HostDistance.LOCAL, 10)
	    .setCoreConnectionsPerHost(HostDistance.REMOTE, 2)
	    .setMaxConnectionsPerHost( HostDistance.REMOTE, 4);
		poolingOptions
	    .setConnectionsPerHost(HostDistance.LOCAL,  4, 10)
	    .setConnectionsPerHost(HostDistance.REMOTE, 2, 4);
		//when a connection has been idle for a given amount of time,
		//the driver will simulate activity by writing a dummy request to it.
		//This feature is enabled by default. The default heart beat interval is 30 seconds
		poolingOptions.setHeartbeatIntervalSeconds(60);
		
		try{
				//change the IP address to the Ip address where docker is running
				Cluster cluster = Cluster.builder().addContactPoint("172.17.0.2").withPoolingOptions(poolingOptions)
				    .build();
				cluster.connect();
			} catch (NoHostAvailableException h){
			System.out.println("check host connection (" + h.getMessage() + ")" );
		} 
	}

	public boolean isNew()
	{
		return isNew;
	}

	//BIMServer starts the transaction if yes => need to start session else 
	public BimTransaction startTransaction() 
	{
		if (useTransactions) {
			try {
				return new BerkeleyTransaction(session.init());
			} catch (NoHostAvailableException h){
				System.out.println("check host connection (" + h.getMessage() + ")" );
			} catch (AuthenticationException a){
				System.out.println("Contacted host "+ a.getHost() +" not available at " + a.getAddress() );
			}
		}
		return null;
	}

	//Create Table
	public boolean createTable(String tableName, DatabaseSession databaseSession, boolean transactional) throws BimserverDatabaseException
	{
		if (tables.containsKey(tableName)) {
			throw new BimserverDatabaseException("Table " + tableName + " already created");
		}
		boolean finalTransactional = session.isClosed();
		//select a key space created and add the table in it
		Metadata metadata =cluster.getMetadata();
		System.out.printf("Connected to cluster : %s \n",metadata.getClusterName());
		KeyspaceMetadata database = metadata.getKeyspace(mykeyspace);
		session = cluster.connect(); 
		if (database == null) {
			return false;
		}
		tables.put(tableName, new TableWrapper(database, finalTransactional));
		return true;
	}
	
	//create Index table
	public boolean createIndexTable(String tableName, DatabaseSession databaseSession, boolean transactional) throws BimserverDatabaseException 
	{
		if (tables.containsKey(tableName)) {
			throw new BimserverDatabaseException("Table " + tableName + " already created");
		}
		boolean finalTransactional = session.isClosed();
		Metadata metadata =cluster.getMetadata();
		//System.out.printf("Connected to cluster : %s \n",metadata.getClusterName());
		session = cluster.connect();
		KeyspaceMetadata database = metadata.getKeyspace(mykeyspace);
		if (database == null) {
			return false;
		}
		tables.put(tableName, new TableWrapper(database, finalTransactional));
		return true;
	}
	
	//open table - table which has all keyspaces/dbs/tables
	public boolean openTable(String tableName, boolean transactional) throws BimserverDatabaseException
	{
		if (tables.containsKey(tableName)) {
			throw new BimserverDatabaseException("Table " + tableName + " already opened");
		}
		boolean finalTransactional = session.isClosed();
		Metadata metadata =cluster.getMetadata();
		//System.out.printf("Connected to cluster : %s \n",metadata.getClusterName());
		session = cluster.connect();
		KeyspaceMetadata database = metadata.getKeyspace(mykeyspace);
		if (database == null) {
			throw new BimserverDatabaseException("Table " + tableName + " not found in database");
		}
		tables.put(tableName, new TableWrapper(database, finalTransactional));
		return true;
	}

	//open Index table - table which has all keyspaces/dbs/tables
	public void openIndexTable(String tableName, boolean transactional) throws BimserverDatabaseException
	{
		if (tables.containsKey(tableName)) {
			throw new BimserverDatabaseException("Table " + tableName + " already opened");
		}
		boolean finalTransactional = session.isClosed();
		Metadata metadata =cluster.getMetadata();
		//System.out.printf("Connected to cluster : %s \n",metadata.getClusterName());
		session = cluster.connect();
		KeyspaceMetadata database = metadata.getKeyspace(mykeyspace);
		if (database == null) {
			throw new BimserverDatabaseException("Table " + tableName + " not found in database");
		}
		tables.put(tableName, new TableWrapper(database, finalTransactional));
	}
	
	//get a respective/requested name/key 
	private KeyspaceMetadata getDatabase(String tableName) throws BimserverDatabaseException
	{
		return getTableWrapper(tableName).getDatabase();
	}

	//get a respective value 
	private TableWrapper getTableWrapper(String tableName) throws BimserverDatabaseException
	{
		TableWrapper tableWrapper = tables.get(tableName);
		if (tableWrapper == null) {
			throw new BimserverDatabaseException("Table " + tableName + " not found");
		}
		return tableWrapper;
	}

	//BIMServer session is changed to Cassandra session and it is getting the cluster 
	//getCluster returns a cluster object this session is a part off
	private Session getTransaction(DatabaseSession databaseSession) 
	{
		if (databaseSession != null) {
			BerkeleyTransaction BerkeleyTransaction = (BerkeleyTransaction) databaseSession.getBimTransaction();
			if (BerkeleyTransaction != null) {
				return BerkeleyTransaction.getCluster();
			}
		}
		return null;
	}

	//Closes the values first and then closes the whole table
	public void close()
	{
		for (TableWrapper tableWrapper : tables.values()) {
			tableWrapper.getDatabase().equals(session.closeAsync());
		}
		if (tables.isEmpty()) {
			session.close();
		}
	}

	//Do we need a lock mode?
	//if yes -
	//If => tableWrapper is transactional then return lockmode is in read_commited 
	//else => lockmode is in read_uncommited
	
	//Checking if the tableWrapper is in use in that session 
	//if not in use => then creates a session and calls the cluster object as in the methos called
	//else returns null
	public Session getTransaction(DatabaseSession databaseSession, TableWrapper tableWrapper) 
	{
		return tableWrapper.isTransactional() ? getTransaction(databaseSession) : null;
	}
	
	//gets the key and value from the table requested
	@Override
	public byte[] get(String tableName, byte[] keybytes, DatabaseSession databaseSession) throws BimserverDatabaseException
	{		
		String key =  Bytes.toHexString(keybytes);
		try {
				TableWrapper value = getTableWrapper(tableName);
				ResultSet r = session.execute(key, value);
				List<Row> rs =	r.all();
				byte[] rs1= Bytes.getArray((ByteBuffer) rs);
				return rs1;

		} catch (NoHostAvailableException h){
			System.out.println("check host connection (" + h.getMessage() + ")" );
		} catch (QueryExecutionException qe){
			LOGGER.error("Execution error "+ qe.getMessage() +"cause: "+ qe.getCause());
		} catch (QueryValidationException qv){
			LOGGER.error("Validation error " + qv.getMessage() + "cause: " + qv.getCause());
		} catch (UnsupportedFeatureException uf){
			LOGGER.error("Message : " + uf.getMessage() + "beacause of the CURRENT version " + uf.getCurrentVersion());
		}
		return null;
	}

	//creating a list which has the duplicate copies of the execution query 
	@Override
	public List<byte[]> getDuplicates(String tableName,byte[] keybytes, DatabaseSession databaseSession) throws BimserverDatabaseException
	{

		try {
				String key =  Bytes.toHexString(keybytes);
				TableWrapper value = getTableWrapper(tableName);
				ResultSet r = session.execute(key, value);
				List<Row> rs= r.all();
				try {
						List<Row> result = new ArrayList<Row>();
						while(result.isEmpty())
						{
							result.addAll(rs);
						}
						//result is converted from List<Row> to byte[]
						byte[] res1 = Bytes.getArray((ByteBuffer) result);
						//have to convert byte[] to list<byte[]>
						List<byte[]> res = Arrays.asList(res1); 
						return res; //this is returning byte[] but it shld return list bytearray
				} finally {
					session.close();
				}
			} catch (NoHostAvailableException h){
				System.out.println("check host connection (" + h.getMessage() + ")" );
			} catch (QueryExecutionException qe){
				LOGGER.error("Execution error "+ qe.getMessage() +"cause: "+ qe.getCause());
			} catch (QueryValidationException qv){
				LOGGER.error("Validation error " + qv.getMessage() + "cause: " + qv.getCause());
			} catch (UnsupportedFeatureException uf){
				LOGGER.error("Message : " + uf.getMessage() + "beacause of the CURRENT version " + uf.getCurrentVersion());
			}
		return null;
	}
	
	public void sync() 
	{
//			try {
//				environment.sync();
//				environment.flushLog(true);
//				environment.evictMemory();
//			} catch (BimserverDatabaseException e) {
//				LOGGER.error("", e);
//			}
	}

	//checks if it contains the table for the key tableName
	@Override
	public boolean containsTable(String tableName)
	{
		try {
				if(tables.containsKey(tableName))
				{
					TableWrapper value = getTableWrapper(tableName);
					return tables.containsValue(value);
				}
				
		}catch (BimserverDatabaseException db) {
			LOGGER.error("", db);
		}catch (ClassCastException c) {
			LOGGER.error("", c.getCause());
		}catch (NullPointerException n) {
			LOGGER.error("", n.getCause());
		}
		return false;
	}

	//record iterator
	@Override
	public RecordIterator getRecordIterator(String tableName, DatabaseSession databaseSession) throws BimserverDatabaseException {
		
		try {

			TableWrapper tableWrapper = getTableWrapper(tableName);
			
			if(getTransaction(databaseSession, tableWrapper).isClosed())
			{
				System.out.println("The transaction session is closed, Initiate the transaction \n");
			}else
			{
				//initiating the transaction
				getTransaction(databaseSession, tableWrapper).init();
				System.out.println("Session initiated \n");
			}
			
			BerkeleyRecordIterator berkeleyRecordIterator = new BerkeleyRecordIterator(tableWrapper , this);
			return berkeleyRecordIterator;
		}catch (BimserverDatabaseException db) {
			LOGGER.error("", db);
		}
		return null;
	}
	
	@Override
	public SearchingRecordIterator getRecordIterator(String tableName, byte[] mustStartWith, byte[] startSearchingAt, DatabaseSession databaseSession) throws BimserverLockConflictException, BimserverDatabaseException
	{
		
		try {
			TableWrapper tableWrapper = getTableWrapper(tableName);
			//cursor = tableWrapper.getDatabase().openCursor(getTransaction(databaseSession, tableWrapper), getCursorConfig(tableWrapper));
			if(getTransaction(databaseSession, tableWrapper).isClosed())
			{
				System.out.println("The transaction session is closed, Initiate the transaction \n");
			}else
			{
				//initiating the transaction
				getTransaction(databaseSession, tableWrapper).init();
				System.out.println("Session initiated \n");
			}
			BerkeleySearchingRecordIterator berkeleySearchingRecordIterator = new BerkeleySearchingRecordIterator( this, tableWrapper,mustStartWith, startSearchingAt);
			return berkeleySearchingRecordIterator;
		} catch (BimserverLockConflictException e) {
			if (session != null) {
				try {
					session.close();
					throw e;
				} catch (BimserverDatabaseException e1) {
					LOGGER.error("", e1);
				}
			}
		} catch (BimserverDatabaseException e1) {
			LOGGER.error("", e1);
		}
		return null;
}
	//Count the number of tables in the big table.
	@Override
	public long count(String tableName) {
		try {
			return getDatabase(tableName).getTables().size();
		} catch (BimserverDatabaseException e) {
			LOGGER.error("", e);
		}
		return -1;
	}

	@Override
	public byte[] getFirstStartingWith(String tableName, byte[] key, DatabaseSession databaseSession) throws BimserverLockConflictException, BimserverDatabaseException {
		SearchingRecordIterator recordIterator = getRecordIterator(tableName, key, key, databaseSession);
		try {
			Record record = recordIterator.next(key);
			if (record == null) {
				return null;
			}
			return record.getValue();
		} finally {
			recordIterator.close();
		}
}
	//delete the key table 
	public void delete(String tableName, byte[] key, DatabaseSession databaseSession) throws BimserverLockConflictException {
		try {
			if(containsTable(tableName)){
			delete(tableName, key, databaseSession);
			}else
			{
				System.out.println("Table not found \n");
				
			}
		}catch (ReadTimeoutException rt) {
		    throw new BimserverLockConflictException(rt);
	    }catch (WriteTimeoutException wt) {
	    throw new BimserverLockConflictException(wt);
	    }catch (UnsupportedOperationException e) {
			LOGGER.error("", e);
		} catch (IllegalArgumentException e) {
			LOGGER.error("", e);
		} catch (BimserverDatabaseException e) {
			LOGGER.error("", e);
		}
	}
	
	//delete the respective index table 
	@Override
	public void delete(String indexTableName, byte[] featureBytesOldIndex, byte[] array, DatabaseSession databaseSession) throws BimserverLockConflictException {
		try {
			try {
					if(containsTable(indexTableName)){
						delete(indexTableName, featureBytesOldIndex, databaseSession);
					}else
					{
						System.out.println("Table not found \n");
						
					}
				} finally {
				session.close();
				}
		} catch (ReadTimeoutException rt) {
		    throw new BimserverLockConflictException(rt);
	    }catch (WriteTimeoutException wt) {
	    	throw new BimserverLockConflictException(wt);
	    } catch (UnsupportedOperationException e) {
			LOGGER.error("", e);
		} catch (IllegalArgumentException e) {
			LOGGER.error("", e);
		} catch (BimserverDatabaseException e) {
			LOGGER.error("", e);
		}
	}

	//returns the address of the located host
	@Override
	public String getLocation() {
		final Metadata metadata = cluster.getMetadata();
		try{
			for (final Host host : metadata.getAllHosts())
			{
				return host.getAddress().toString();
				
			}
		}  catch (NoHostAvailableException h){
			System.out.println("check host connection (" + h.getMessage() + ")" );
		} catch (ConnectionException e)
		{
			System.out.println("connection  porblem ( with host "+ e.getHost() +"  because of "  + e.getMessage() + ")" );
		}
		return "unknown";
	}

	//saves and closes after the transaction is done
	@Override
	public void commit(DatabaseSession databaseSession) throws BimserverLockConflictException, BimserverDatabaseException {
		Session bdbTransaction = getTransaction(databaseSession);
		try {
			bdbTransaction.close();
		} catch (ReadTimeoutException rt) {
		    throw new BimserverLockConflictException(rt);
	    }catch (WriteTimeoutException wt) {
	    throw new BimserverLockConflictException(wt);
	    }
	}

	//stores the Key and value in the table
	@Override
	public void store(String tableName, byte[] key, byte[] value, DatabaseSession databaseSession) throws BimserverDatabaseException, BimserverLockConflictException {
		store(tableName, key, value, 0, value.length, databaseSession);
	}
	
	//stores the Key and value in the table
	@Override
	public void store(String tableName, byte[] key, byte[] value, int offset, int length, DatabaseSession databaseSession) throws BimserverDatabaseException, BimserverLockConflictException {
		
			try {
				String dbkey =  Bytes.toHexString(key);
				TableWrapper tableWrapper = getTableWrapper(tableName);
				if (tableWrapper.equals(tableName))
				{
					ResultSet r = session.execute(dbkey, value);
					System.out.println("Successfully stored key and value : "+ r.all());
				}
				} catch (ReadTimeoutException rt) {
					throw new BimserverLockConflictException(rt);
				} catch (WriteTimeoutException wt) {
					throw new BimserverLockConflictException(wt);
				}	
		}

	//If the Key exists, cannot overwrite
	@Override
	public void storeNoOverwrite(String tableName, byte[] key, byte[] value, DatabaseSession databaseSession) throws BimserverDatabaseException, BimserverLockConflictException, BimserverConcurrentModificationDatabaseException {
		storeNoOverwrite(tableName, key, value, 0, value.length, databaseSession);
	}
	
	//If the key exists, then cannot overwrite
	@Override
	public void storeNoOverwrite(String tableName, byte[] key, byte[] value, int index, int length, DatabaseSession databaseSession) throws BimserverDatabaseException, BimserverLockConflictException, BimserverConcurrentModificationDatabaseException {
		try 
		{
			String dbkey =  Bytes.toHexString(key);
			ResultSet r = session.execute(dbkey, value);
			if (r.isFullyFetched()) 
			{
				ByteBuffer keyBuffer = ByteBuffer.wrap(key);
				if (dbkey.length() == 16)
				{
					int pid = keyBuffer.getInt();
					long oid = keyBuffer.getLong();
					int rid = -keyBuffer.getInt();
					throw new BimserverConcurrentModificationDatabaseException("Key exists: pid: " + pid + ", oid: " + oid + ", rid: " + rid);
				}
				else
				{
					throw new BimserverConcurrentModificationDatabaseException("Key exists: " );
				}
			}
		}catch (ReadTimeoutException rt) {
		    throw new BimserverLockConflictException(rt);
	    }catch (WriteTimeoutException wt) {
	    throw new BimserverLockConflictException(wt);
	    } 
	}
	
	//Driver Version Number
	@Override
	public String getType() {
		 		return "apache casssandra DB Java Edition " + session.getCluster().getDriverVersion().toString();
	}

	//returns the No of Key-value mappings in the Map
	@Override
	public long getDatabaseSizeInBytes() {
		long sizedatabase = 0;
		try{
					sizedatabase = tables.size();
					
		}
		finally {
			System.out.println("Returns all the key-value mappings of the Map ");
			session.close();
		}
		return sizedatabase;
	}

	//Returns all the table names stored in the Map
	public Set<String> getAllTableNames() {
		return new HashSet<String>(tables.keySet());
	}
	
	public synchronized void incrementReads(long reads) {
		this.reads += reads;
		if (this.reads / 100000 != lastPrintedReads) {
			LOGGER.info("reads: " + this.reads);
			lastPrintedReads = this.reads / 100000;
		}
	}
	
	@Override
	public synchronized void incrementCommittedWrites(long committedWrites) {
		this.committedWrites += committedWrites;
		if (this.committedWrites / 100000 != lastPrintedCommittedWrites) {
			LOGGER.info("writes: " + this.committedWrites);
			lastPrintedCommittedWrites = this.committedWrites / 100000;
		}
	}
}
