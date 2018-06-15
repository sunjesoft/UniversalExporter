package com.sunjesoft.util;

import java.io.FileReader;
import java.util.Iterator;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


import com.sunjesoft.util.ImportConfigItem.TableList;

public class ImportConfigItem {
protected static Logger logger = Logger.getLogger(ImportConfigItem.class.getName());
	
	public class TableList {
		private String tableName; 
		private String inFileName;
		private String fieldSeperator ; 
		private String rowSeparator ;
    private String SQLBeforeHook ; 
    private String SQLAfterHook ; 

		
		public TableList ( String table, String inFileName, String fieldSep, String rowSep, String before, String after ) { 
			this.setTableName(table);

			this.setInFileName(inFileName); 
			this.setFieldSeperator(fieldSep) ; 
			this.setRowSeparator(rowSep) ; 
      this.setSQLBeforeHook ( before ) ;
      this.setSQLAfterHook ( after ) ;
		}

		public String getTableName() {
			return tableName;
		}

		public void setTableName(String tableName) {
			this.tableName = tableName;
		}

		public String getInFileName() {
			return inFileName;
		}

    public String getSQLBeforeHook () { 
      return SQLBeforeHook ; 
    } 
 
    public String getSQLAfterHook () { 
      return SQLAfterHook ;
    } 

    public void setSQLBeforeHook ( String beforeHook ) { 
      this.SQLBeforeHook = beforeHook ;

    } 

    public void setSQLAfterHook ( String afterHook ) { 
      this.SQLAfterHook = afterHook ;
    } 

		public void setInFileName(String inFileName) {
			this.inFileName = inFileName;
		}

		public String getFieldSeperator() {
			return fieldSeperator;
		}

		public void setFieldSeperator(String fieldSeperator) {
			this.fieldSeperator = fieldSeperator;
		}

		public String getRowSeparator() {
			return rowSeparator;
		}

		public void setRowSeparator(String rowSeparator) {
			this.rowSeparator = rowSeparator;
		}
	}

	private Vector<TableList> tableList ;
	private String url ; 
	private String driver; 
	private String user ; 
	private String password; 
	
	public  String loggingURL ;
	public  String loggingDriver; 
	public  String loggingUser; 
	public  String loggingPassword; 



	public ImportConfigItem ( ) { 
		setQueryList(new Vector <TableList> ());
		setUrl("") ;
		setDriver("");
		setUser("") ; 
		setPassword("") ;
	}
	
	
	public String toString () { 
		
		String tmp = "\n\n"; 
		
		tmp += "=============DESTINATION INFORMATION=============";
		tmp += "\nDriver : [" + this.driver + "]" ; 
		tmp += "\nURL : [" + this.url + "]";
		tmp += "\nUser : [" + this.user + "]";
		tmp += "\nPassword : [" + this.password + "]\n\n"; 

		tmp += "===============SOURCE  INFORMATION===============\n";
	    for ( int i = 0; i < this.getTableList().size () ; i ++ ) { 
	    	tmp += "[" + i + "] == TableName :  " + this.getTableList().get(i).tableName + "\tFileName : " + this.getTableList().get(i).inFileName+ "\n" ;
	    }
		return tmp;
	
	}

		
	public void loadJSONFromFile ( String fileName ) {
			
		JSONParser parser = new JSONParser();
		
		try { 
			FileReader fileReader = new FileReader ( fileName );
			
			Object obj = parser.parse(fileReader);
			JSONObject jsonObject = (JSONObject)obj ; 
			
			JSONObject connectionInfo = (JSONObject)jsonObject.get("DEST");
			this.setUrl((String)connectionInfo.get("JDBCURL"));
			this.setDriver((String)connectionInfo.get("JDBCDriver"));
			this.setUser((String) connectionInfo.get("User"));
			this.setPassword((String) connectionInfo.get("Password"));

			JSONArray dataArray = (JSONArray)jsonObject.get( "SOURCE" ); 
			Iterator<JSONObject> iterator = dataArray.iterator();
			
			while ( iterator.hasNext() ) {
			
				JSONObject data = iterator.next();
				TableList tableList = new TableList (
						(String)data.get("TableName") , 
						(String)data.get("CSVFileName") ,
						(String)data.get("FieldSeperator") ,
						(String)data.get("RowSeperator") , 
            (String)data.get("SQLBeforeHook" ) , 
            (String)data.get("SQLAfterHook") 
				); 
				
       
				
				this.getTableList().add( tableList ) ;
				
			}
			logger.info( this );
			logger.info("Configuration was parsed sucessfully\n");
			System.out.println("=================IMPORT  START===================");
			
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



	public void setQueryList(Vector<TableList> vector) {
		this.setTableList(vector);
	}


	public Vector<TableList> getTableList() {
		return tableList;
	}


	public void setTableList(Vector<TableList> tableList) {
		this.tableList = tableList;
	}
	
    
}

