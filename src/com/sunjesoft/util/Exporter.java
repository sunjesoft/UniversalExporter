package com.sunjesoft.util;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.BufferedReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.sql.Clob;
import java.sql.Blob;
import java.text.SimpleDateFormat;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.commons.codec.binary.Hex;


import com.sunjesoft.util.ExporterConfigItem.QueryList;
import com.sunjesoft.util.ExporterConfigItem; 

public class Exporter implements Runnable{


	ExporterConfigItem configItem;
	Connection cn;
	int index; 
	private static final Logger logger = Logger.getLogger(Exporter.class.getName());

	public Exporter ( ExporterConfigItem configItem, int index ) { 
		this.configItem = configItem;
		this.index = index; 
	}


	private static String convertValue(String aValue)
	{
		return aValue.replace("\"", "\"\"");
	}


	public void ExportData ( int idx ) throws SQLException{

		CreateSourceConnection ( idx) ;

		QueryList query = configItem.getQueryElement2(idx) ;

		int res = 0;

		try {
			PrintWriter sWriter = new PrintWriter(new FileOutputStream(query.getOutFileName()));
			Statement sStmt = cn.createStatement();
            sStmt.setFetchSize( 1024 );

			ResultSet sRs = sStmt.executeQuery(query.getQuery());
			ResultSetMetaData sMeta = sRs.getMetaData();

			SimpleDateFormat sTimeFormatter      = new SimpleDateFormat( "HH:mm:ss" );
			SimpleDateFormat sDateFormatter      = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
			SimpleDateFormat sTimestampFormatter = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSSSSS" );

			Date sDate;
			String sValue;
			int cnt  = 0 ; 

			while ( sRs.next())  { 
				for (int i=1; i<=sMeta.getColumnCount(); i++)
				{
					if (i > 1)
					{
						sWriter.print(query.getFieldSeparator());
					}

					sWriter.print("\"");

                    Object dummyObj =  sRs.getObject(i);
					if( sRs.wasNull() == true )
					{
						sValue = "";
					}
					else
					{
						if (sMeta.getColumnType(i) == Types.CHAR )
						{
							sValue = convertValue(sRs.getString(i));
                            if( sValue.length() == 0 )
                            {
                                int sLength = sMeta.getColumnDisplaySize( i );
                                for( int ii = 0; ii < sLength; ii++ )
                                {
                                    sValue += " ";
                                }
                            }
						}

						else if(sMeta.getColumnType(i) == Types.TIME )
                        {
                            sValue = sTimeFormatter.format( sRs.getTime(i) );
							sValue = sValue.substring( 0, sValue.length() );

                        }
                        else if(sMeta.getColumnType(i) == Types.DATE )
                        {
                            sValue = sDateFormatter.format( sRs.getDate(i) );
							sValue = sValue.substring( 0, sValue.length() );

                        }
						else if( sMeta.getColumnType(i) == Types.TIMESTAMP )
						{
                            if( sMeta.getColumnTypeName( i ) == "DATE" )
                            {
                                sValue = sDateFormatter.format( sRs.getDate(i) );
                            }
                            else
                            {
                                sValue = sTimestampFormatter.format( sRs.getDate(i) );
                            }
							sValue = sValue.substring( 0, sValue.length() );
						}
                        else if( sMeta.getColumnType(i) == Types.CLOB )
                        {
                            sValue = clobToString( i, sRs.getClob(i) );
                        }
                        else if( sMeta.getColumnType(i) == Types.BLOB )
                        {
                            sValue = blobToHexString( i, sRs.getBlob(i) );
                        }
						else
						{
							sValue = sRs.getString(i);
						}
					}
					sWriter.print(sValue);
					sWriter.print("\"");
				}
				sWriter.print ( query.getRowSeparator());
				cnt++;
			}
			logger.info("[" + idx + "] " + cnt + " Record Dumped");
			sWriter.flush();
			sWriter.close();
			sRs.close();
			sStmt.close();

		} catch (SQLException e) {
			logger.fatal ( "[" + idx + "] " + e.getMessage());
			cn.close();
			System.exit(-1);
		} catch (Exception e) {
			String sqlErr = e.toString();
			logger.fatal ( "[" + idx + "] " + e.getMessage());
			cn.close();
			System.exit(-1);
		}finally {
			cn.close();
		}
	}

	private void CreateSourceConnection ( int index) { 

		String password = configItem.getPassword () ; 
		try {
			Class.forName( configItem.getDriver () );
			String dec = AES.decrypt ( password, AES.encryptionKey ) ; 
			cn = DriverManager.getConnection( configItem.getUrl(), configItem.getUser(), dec.trim() );
		} catch (Exception e) {
			// TODO Auto-generals -ated catch block
			logger.fatal( "[" + index + "] SOURCE CONNECTION FAILED =====");
			logger.fatal( "[" + index + "] " + e ) ;
			logger.fatal( "[" + index + "] Program Exit");
			System.exit( -1 );
		}
	}

    private String clobToString( int colIdx, Clob data) {
        StringBuilder sb = new StringBuilder();
        char[]      buff = new char[1024];
        try {
            Reader reader = data.getCharacterStream();
            BufferedReader br = new BufferedReader(reader);
            // int bytesRead = 0;
            
            // while( (bytesRead = br.read( buff, 0, 1024 ) ) != -1 )
            // {
            //     sb.append( buffer, 0, bytesRead );
            // }
            // br.close();
            
            String line;
            while(null != (line = br.readLine())) {
                sb.append(line);
            }
            br.close();
            
        } catch (SQLException e) {
            logger.fatal( "[" + colIdx + "] " + e);
        } catch (IOException e) {
            logger.fatal( "[" + colIdx + "] " + e);
        }
        return sb.toString();
    }

    private String blobToHexString( int colIdx, Blob data) {
        byte[] blobBytes = null;
        try {
            blobBytes = data.getBytes(1, (int)data.length());
        } catch (SQLException e) {
            logger.fatal( "[" + colIdx + "] " + e);
        }
        return Hex.encodeHexString(blobBytes).toUpperCase();
    }


	@Override
		public void run() {
			// TODO Auto-generated method stub
			try{
				ExportData ( index );
			}catch(SQLException e){
				e.printStackTrace();
			}
		}
}

