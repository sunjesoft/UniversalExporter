package com.sunjesoft.util;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.sql.Timestamp;
import java.util.Vector;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import javax.activation.UnsupportedDataTypeException;
import com.opencsv.CSVReader;
import org.apache.log4j.Logger;
import org.apache.commons.codec.binary.Hex;

import java.io.File;
import java.io.FileInputStream;

import com.sunjesoft.util.ImportConfigItem.TableList;

public class Importer implements Runnable {

	ImportConfigItem configItem ;
	Connection cn = null; 
    String     databaseName;
	int index; 
	private static final Logger logger = Logger.getLogger(Importer.class.getName());

	private PreparedStatement mPrepareStatement;


	public class Element {
		public int  mIndex; 
		public String mColumnName ;
		public String mColumnTypeName;
		public int    mColumnType;
	};

	Vector<Element> mElements = new Vector<Element>() ;


	public Importer ( ImportConfigItem configItem, int index ) { 
		this.configItem = configItem;
		this.index = index; 
	}

	public void ImportData ( int index) throws SQLException {

		TableList tableList = configItem.getTableList().get(index);
        java.sql.Timestamp tmpTimestamp;

		try {
			RunSQL ( index, 1, configItem.getTableList().get(index).getSQLBeforeHook() ) ;

			CSVReader reader = new CSVReader ( new FileReader ( tableList.getInFileName()) );
			String [] newLine;

			while ( (newLine = reader.readNext()) != null ) { 

				for ( int i = 0 ;i < newLine.length ; i ++ ) {

					if ( mElements.get(i).mColumnType == Types.CHAR  || 
                         mElements.get(i).mColumnType == Types.VARCHAR     || 
                         mElements.get(i).mColumnType == Types.TIME ||
                         mElements.get(i).mColumnType == Types.LONGVARCHAR ||
                         mElements.get(i).mColumnType == Types.TIMESTAMP )  
                    { 
						mPrepareStatement.setString( i + 1  ,  newLine[i]);
					} 
                    else if(  mElements.get(i).mColumnType == Types.DATE )
                    {
                        if( databaseName.compareTo( "GOLDILOCKS" ) == 0 )
                        {
                            tmpTimestamp = Timestamp.valueOf( newLine[i] );
                            mPrepareStatement.setTimestamp( i + 1 , tmpTimestamp );
                        }
                        else
                        {
                            mPrepareStatement.setString( i + 1  ,  newLine[i]);
                        }
                    }
                    else if ( mElements.get(i).mColumnType == Types.NUMERIC  ||
                              mElements.get(i).mColumnType == Types.DECIMAL  || 
                              mElements.get(i).mColumnType == Types.INTEGER  ||
                              mElements.get(i).mColumnType == Types.DOUBLE   ||
                              mElements.get(i).mColumnType == Types.FLOAT ) 
                    { 
						BigDecimal decimal = new BigDecimal ( newLine[i] );
						mPrepareStatement.setBigDecimal(i + 1 , decimal);
					} 
                    else if( mElements.get(i).mColumnType == Types.LONGVARBINARY )
                    {
                        mPrepareStatement.setBytes( i + 1 , Hex.decodeHex( newLine[i] ) );
                    }
                    else
                    { 
						logger.error ("Invalid ... parameter " ) ;
					}  
				}
				mPrepareStatement.executeUpdate();		
			}

			mPrepareStatement.close();

		} catch (SQLException e) {
			logger.fatal( "[" + index + "] " + e.getMessage());
			mPrepareStatement.close();
			cn.close();
			System.exit( -1 );
		} catch (Exception e) {
			logger.fatal("[" + index + "] " + e.getMessage());
			mPrepareStatement.close();
			cn.close();
			System.exit( -1 );
		}finally {
			mPrepareStatement.close();
			RunSQL ( index, 2, configItem.getTableList().get(index).getSQLAfterHook() ) ;
			cn.close();
		}
	}

	public void RunSQL ( int idx, int cnt, String aSQL ) { 
		if (aSQL.equals("")) { 
			if(cnt == 1)
				logger.info( "[" + idx + "] BeforeHook Query is empty string");
			else
				logger.info( "[" + idx + "] AfterHook Query is empty string");
			return ;
		} 
		try { 
			Statement sStmt = cn.createStatement();
			int r = sStmt.executeUpdate(  aSQL  ) ; 
			if(cnt == 1){
				logger.info( "[" + idx + "] BeforeHook Query : " + aSQL);
				logger.info( "[" + idx + "] " + r + " rows affected by BeforeHook Query");
			}
			else{
				logger.info( "[" + idx + "] AfterHook Query : " + aSQL);
				logger.info( "[" + idx + "] " + r + " rows affected by AfterHook Query");
			}

		} catch (Exception e) { 
			if(cnt == 1)
				logger.fatal ( "[" + idx + "] Could not BeforeHook running " + aSQL  )  ;
			else
				logger.fatal ( "[" + idx + "] Could not AfterHook running " + aSQL ) ;
			logger.fatal ( e ) ;
			logger.fatal ( "[" + idx + "] Program Exit");
			System.exit ( -1 ) ;
		} 
	} 


	public void Load ( int index){// throws SQLException {
		CreateConnection ( index) ;


		FileInputStream fis = null;
		Statement sStmt = null;
		ResultSet rs = null;

		try{
			TableList tableList = configItem.getTableList().get(index);
			fis = new FileInputStream(tableList.getInFileName());

			String sql = "SELECT * FROM " + tableList.getTableName () + " WHERE 1 = 0 ";

			sStmt = cn.createStatement();
			rs = sStmt.executeQuery(sql);
			ResultSetMetaData rsmd = rs.getMetaData();
            databaseName = cn.getMetaData().getDatabaseProductName();

			for ( int i = 1 ; i < rsmd.getColumnCount() +1  ; i ++ )
			{
				Element element = new Element () ;
				element.mIndex = i ; 
				element.mColumnName     = rsmd.getColumnName( i );
				element.mColumnTypeName = rsmd.getColumnTypeName (i);
                element.mColumnType     = rsmd.getColumnType( i );

				mElements.addElement(element);
			}

			String sql2 = getQueryString( tableList.getTableName() , rsmd.getColumnCount() );
			mPrepareStatement = cn.prepareStatement(sql2);

            logger.info ( "[" + index + "] Database : " + databaseName );
			logger.info ( "[" + index + "] Table    : " + tableList.getTableName () );
			logger.info ( "[" + index + "] Query    : " + sql2 );

			rs.close();
			sStmt.close();
		}catch(FileNotFoundException e){
			
			try{
				sStmt.close();
				rs.close();
				cn.close();
				fis.close();
			}catch(Exception e1){
			}
			logger.fatal( "[" + index + "] ** WARNING ** FILE NOT FOUNDED");
			logger.fatal( "[" + index + "] " + e );
			logger.fatal( "[" + index + "] Program Exit");
			System.exit( -1 );

		}catch(SQLException e){
			try{
				sStmt.close();
				rs.close();
				cn.close();
				fis.close();
			}catch(Exception e1){
			}
			logger.fatal( "[" + index + "] ** WARNING ** SQL ERROR OCCURED");
			logger.fatal( "[" + index + "] " + e );
			logger.fatal( "[" + index + "] Program Exit");
			System.exit( -1 );
		}

	}

	private void CreateConnection ( int index) { 
		try {
			String  password = configItem.getPassword () ;

			Class.forName( configItem.getDriver () );
            
			String  dec = AES.decrypt ( password, AES.encryptionKey ) ;
			cn = DriverManager.getConnection( configItem.getUrl(), configItem.getUser(), dec.trim() );
		} catch (Exception e )  { 
			try{
				cn.close();
			}catch(Exception e1){
			}

			// TODO Auto-generals -ated catch block
			logger.fatal( "[" + index + "] ** WARNING ** DESTINATION CONNECTION FAILED");
			logger.fatal( "[" + index + "] " + e ) ;
			logger.fatal( "[" + index + "] Program Exit");
			System.exit( -1 );
		} 
	}

	private String getQueryString ( String tableName,  int cnt ) { 
		String sql = " INSERT INTO " + tableName + " VALUES ("; 

		for ( int i = 0 ; i < cnt ; i++ ) {

			if ( i == cnt - 1 ) 
            { 
				sql += "?";
			} 
            else 
			{
				sql += "?, ";
			}
		}

		sql += ")";
		return sql ;
	}


	public void run() {
		try {
			Load (index);
			ImportData(index);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

