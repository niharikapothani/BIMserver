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

import org.bimserver.BimserverDatabaseException;
import org.bimserver.database.BimTransaction;
import org.bimserver.database.BimserverLockConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class CassandraTransaction implements BimTransaction {

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraTransaction.class);
	private final Session transaction;
	private boolean transactionAlive = true;

	public CassandraTransaction(Session transaction) {
		this.transaction = transaction;
	}

	public Session getCluster() {
		return transaction;
	}

	@Override
	public void setName(String name) {
		System.out.println("No name for session in cassandra...have to edit this part of code!");
	}

	@Override
	public void close() {
		if (transactionAlive) {
			rollback();
		}
	}

	@Override
	public void rollback() {
		try {
			transaction.close();
			transactionAlive = false;
		}
		catch(NoHostAvailableException h){
			h.getCause();
			LOGGER.error(getId());
		}
		
	}

	@Override
	public void commit() throws BimserverLockConflictException, BimserverDatabaseException {
		try {
			transaction.close();
			transactionAlive = false;
		}catch (ReadTimeoutException rt) {
		    throw new BimserverLockConflictException(rt);
	    }catch (WriteTimeoutException wt) {
	    throw new BimserverLockConflictException(wt);
	    }catch(NoHostAvailableException h){
			h.getCause();
		}
//		catch (DatabaseException e) {
//			throw new BimserverDatabaseException(e);
//		}
	}

	@Override
	public String getId() {
		return transaction.getLoggedKeyspace();
	}
}