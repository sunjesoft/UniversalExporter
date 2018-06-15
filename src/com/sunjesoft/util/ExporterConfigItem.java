package com.sunjesoft.util;

import java.io.FileReader;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class ExporterConfigItem {
	protected static Logger logger = Logger.getLogger(ExporterConfigItem.class.getName());
	
	public class QueryList {
		private String query; 
		private String outFileName;
		private String fieldSeparator ; 
		private String rowSeparator ;
		
		public QueryList ( String query, String outFileName, String fieldSep, String rowSep ) { 
			this.query = query;
			this.outFileName = outFileName ; 
			this.fieldSeparator = fieldSep ; 
			this.rowSeparator = rowSep ; 
		}
		
		public String getQuery() {
			return query;
		}
		
		public void setQuery(String query) {
			this.query = query;
		}
		
		public String getOutFileName() {
			return outFileName;
		}
		
		public void setOutFileName(String outFileName) {
			this.outFileName = outFileName;
		}
		
		public String getFieldSeparator() {
			return fieldSeparator;
		}
				
		public void setFieldSeparator(String fieldSeparator) {
			this.fieldSeparator = fieldSeparator;
		}
		
		public String getRowSeparator() {
			return rowSeparator;
		}
		
		public void setRowSeparator(String rowSeparator) {
			this.rowSeparator = rowSeparator;
		} 
	}

	private Vector<QueryList> queryList ;
  private String url ; 
	private String driver; 
	private String user ; 
	private String password; 
	

  public String loggingURL;
  public String loggingDriver; 
  public String loggingUser; 
  public String loggingPassword;



	public ExporterConfigItem ( ) { 
		setQueryList(new Vector <QueryList> ());
		setUrl("") ;
		setDriver("");
		setUser("") ; 
		setPassword("") ;
	}
	
	
	public String toString () { 
		
		String tmp = "\n"; 
		
		tmp += "===============SOURCE INFORMATION=============";
		tmp += "\nDriver : [" + this.driver + "]" ; 
		tmp += "\nURL : [" + this.url + "]";
		tmp += "\nUser : [" + this.user + "]";
		tmp += "\nPassword : [" + this.password + "]\n\n"; 
	
		tmp += "============DESTINATION INFORMATION===========";
	    for ( int i = 0; i < this.queryList.size () ; i ++ ) { 
	    	tmp += "\n[" + i + "] Query :  " + this.queryList.get(i).query;
			tmp += "\n[" + i + "] OutPutFile : " + this.queryList.get(i).outFileName + "\n";
	    }
		return tmp;
	
	}

		
	public void loadJSONFromFile ( String fileName ) {
			
		JSONParser parser = new JSONParser();
		
		try { 
			FileReader fileReader = new FileReader ( fileName );
			
			Object obj = parser.parse(fileReader);
			JSONObject jsonObject = (JSONObject)obj ; 
			
			JSONObject connectionInfo = (JSONObject)jsonObject.get("SOURCE");
			this.setUrl((String)connectionInfo.get("JDBCURL"));
			this.setDriver((String)connectionInfo.get("JDBCDriver"));
			this.setUser((String) connectionInfo.get("User"));
			this.setPassword((String) connectionInfo.get("Password"));

			JSONArray dataArray = (JSONArray)jsonObject.get( "DEST" ); 
			Iterator<JSONObject> iterator = dataArray.iterator();

			while ( iterator.hasNext() ) {

				JSONObject data = iterator.next();
				QueryList queryList = new QueryList (
						(String)data.get("SQL") , 
						(String)data.get("OutputFileName") ,
						(String)data.get("FieldSeperator") ,
						(String)data.get("RowSeperator") 
				); 
				
				this.getQueryList().add( queryList ) ;
				
			}
			logger.info( this );
			logger.info("Configuration was parsed sucessfully\n ");
			
			System.out.println("================EXPORT  START=================");
			
		} catch ( Exception e) { 
			System.out.println(e.toString());
			System.exit(-1);
		}
		
	}


	public String getUrl() {
		return url;
	}


	public void setUrl(String url) {
		this.url = url;
	}


	public String getDriver() {
		return driver;
	}


	public void setDriver(String driver) {
		this.driver = driver;
	}


	public String getUser() {
		return user;
	}


	public void setUser(String user) {
		this.user = user;
	}


	public String getPassword() {
		return password;
	}


	public void setPassword(String password) {
		this.password = password;
	}


	public QueryList getQueryElement2 ( int idx ) { 
		return this.queryList.get( idx );
	}


	public Vector<QueryList> getQueryList() {
		return queryList;
	}

	public void setQueryList(Vector<QueryList> queryList) {
		this.queryList = queryList;
	}
	
    
}

