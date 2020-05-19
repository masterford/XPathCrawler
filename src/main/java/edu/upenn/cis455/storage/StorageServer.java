package edu.upenn.cis455.storage;

import java.util.HashMap;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class StorageServer {

	private DBWrapper myDB;
	private static final StorageServer instance = new StorageServer();
	
	private StorageServer() { 
		
	}
	
	public static StorageServer getInstance() {
		return instance;
	}
	
	public  void init(String directory) {
		try {
			getInstance().myDB = new DBWrapper(directory);
			
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}
	
	/*Add user information to the database using the username as the key  */
	public void addUserInfo(String username, UserVal val) {
		
		//declare database entry key-value pair
		DatabaseEntry value = new DatabaseEntry();
		DatabaseEntry key = new DatabaseEntry(username.getBytes());
		
		myDB.getUserValBinding().objectToEntry(val, value);
		
		//begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			myDB.getUserDB().put(txn, key, value);
			
			txn.commit(); //commit transaction
		} catch (Exception e) {
			if(txn != null) {
				txn.abort();
				txn = null;
			}
		}				
	}
	
	/* Get user information from database using the username as the key  */
	public UserVal getUserInfo(String username) {
		DatabaseEntry value = new DatabaseEntry();
		DatabaseEntry key = new DatabaseEntry(username.getBytes());
		
		//begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			if(myDB.getUserDB().get(txn, key, value, LockMode.DEFAULT) != OperationStatus.SUCCESS){
				return null;
			}
					
			txn.commit(); //commit transaction
		} catch (Exception e) {
			if(txn != null) {
				txn.abort();
				txn = null;
			}
		}	
		return (UserVal) myDB.getUserValBinding().entryToObject(value);
	}
	
	/*Add crawled document to database. URL is the key  */
	public void addDocInfo(String url, DocVal val) {
			
		//declare database entry key-value pair
		DatabaseEntry value = new DatabaseEntry();
		DatabaseEntry key = new DatabaseEntry(url.getBytes());
		
		myDB.getDocValBinding().objectToEntry(val, value);
		
		//begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			myDB.getDocDB().put(txn, key, value);
			
			txn.commit(); //commit transaction
		} catch (Exception e) {
			if(txn != null) {
				txn.abort();
				txn = null;
			}
		}				
	}
	
	/*Retrieve crawled document from the database based on URL key */
	public DocVal getDocInfo(String url) {
		DatabaseEntry value = new DatabaseEntry();
		DatabaseEntry key = new DatabaseEntry(url.getBytes());
		
		//begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			if(myDB.getDocDB().get(txn, key, value, LockMode.DEFAULT) != OperationStatus.SUCCESS){
				return null;
			}
					
			txn.commit(); //commit transaction
		} catch (Exception e) {
			if(txn != null) {
				txn.abort();
				txn = null;
			}
		}	
		return (DocVal) myDB.getDocValBinding().entryToObject(value);
	}
	
	/*Add a new channel to database. Channel Name is the key  */
	public void addChannelInfo(String name, ChannelStorage val) {
			
		//declare database entry key-value pair
		DatabaseEntry value = new DatabaseEntry();
		DatabaseEntry key = new DatabaseEntry(name.getBytes());
		
		myDB.getChannelValBinding().objectToEntry(val, value);
		
		//begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			myDB.getChannelDB().put(txn, key, value);
			
			txn.commit(); //commit transaction
		} catch (Exception e) {
			if(txn != null) {
				txn.abort();
				txn = null;
			}
		}				
	}
	
	public int deleteChannel(String name) {	
		DatabaseEntry key = new DatabaseEntry(name.getBytes());
		
		//begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {			
			if(myDB.getChannelDB().delete(txn, key) != OperationStatus.SUCCESS) {
				return 0;
			} else {
				txn.commit(); //commit transaction
				return 1;
			}											
		} catch (Exception e) {
			if(txn != null) {
				txn.abort();
				txn = null;
			}
		}	
		return 0;
	}
	
	/*Retrieve Channel info from the database based on Channel Name key, null if specified channel doesn't exist */
	public ChannelStorage getChannelInfo(String name) {
		DatabaseEntry value = new DatabaseEntry();
		DatabaseEntry key = new DatabaseEntry(name.getBytes());
		
		//begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			if(myDB.getChannelDB().get(txn, key, value, LockMode.DEFAULT) != OperationStatus.SUCCESS){
				return null;
			}
					
			txn.commit(); //commit transaction
		} catch (Exception e) {
			if(txn != null) {
				txn.abort();
				txn = null;
			}
		}	
		return (ChannelStorage) myDB.getChannelValBinding().entryToObject(value);
	}
	
	public boolean exists(String username) { //check if username already exists in DB
		try {
		    // Create a pair of DatabaseEntry objects. theKey
		    // is used to perform the search. theData is used
		    // to store the data returned by the get() operation.
		    DatabaseEntry theKey = new DatabaseEntry(username.getBytes("UTF-8"));
		    DatabaseEntry value = new DatabaseEntry();
		    
		    if (myDB.getUserDB().get(null, theKey, value, LockMode.DEFAULT) ==
		        OperationStatus.SUCCESS) {
		    	return true;
		        
		    } else {
		        return false;
		    } 
		} catch (Exception e) {
		    e.printStackTrace();
		}
		return true;
	}
	
	public HashMap<String, ChannelStorage> getAllChannels() {
		HashMap<String, ChannelStorage> channels = new HashMap<String, ChannelStorage>();
		Cursor cursor = null;
		try {
		   
		    // Open the cursor. 
		    cursor = myDB.getChannelDB().openCursor(null, null);

		    // Get the DatabaseEntry objects that the cursor will use.
		    DatabaseEntry foundKey = new DatabaseEntry();
		    DatabaseEntry foundData = new DatabaseEntry();

		    // Iterate from the last record to the first in the database
		    while (cursor.getPrev(foundKey, foundData, LockMode.DEFAULT) == 
		        OperationStatus.SUCCESS) {

		        String theKey = new String(foundKey.getData());
		        channels.put(theKey, (ChannelStorage) myDB.getChannelValBinding().entryToObject(foundData));
		    }
		} catch (DatabaseException de) {
		    System.err.println("Error accessing database." + de);
		} finally {
		    // Cursors must be closed.
		    cursor.close();
		}		
		return channels;
	}
	
	public HashMap<String, UserVal> getAllUsers() {
		HashMap<String, UserVal> users = new HashMap<String, UserVal>();
		Cursor cursor = null;
		try {
		   
		    // Open the cursor. 
		    cursor = myDB.getUserDB().openCursor(null, null);

		    // Get the DatabaseEntry objects that the cursor will use.
		    DatabaseEntry foundKey = new DatabaseEntry();
		    DatabaseEntry foundData = new DatabaseEntry();

		    // Iterate from the last record to the first in the database
		    while (cursor.getPrev(foundKey, foundData, LockMode.DEFAULT) == 
		        OperationStatus.SUCCESS) {

		        String theKey = new String(foundKey.getData());
		        users.put(theKey, (UserVal) myDB.getUserValBinding().entryToObject(foundData));
		    }
		} catch (DatabaseException de) {
		    System.err.println("Error accessing database." + de);
		} finally {
		    // Cursors must be closed.
		    cursor.close();
		}		
		return users;
	}
	
	public void close() {
		getInstance().myDB.closeDB();
	}
}
