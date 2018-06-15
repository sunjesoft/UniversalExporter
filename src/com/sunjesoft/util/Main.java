package com.sunjesoft.util;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.sunjesoft.util.ImportConfigItem;
import com.sunjesoft.util.ExporterConfigItem;
import com.sunjesoft.util.Exporter;

public class Main {
  private static final Logger logger = Logger.getLogger(Main.class.getName());
  public static void main(String[] args) {

    if ( args.length != 2 ) {
      logger.error("Missing Configuration File .....");
      System.exit ( -1 ) ;
    }

    logger.info ( args[0] ) ;

    if (args[0].equals("imp") )  { 
      ImportConfigItem item = new ImportConfigItem ();
      item.loadJSONFromFile(args[1]);
      ExecutorService executor = Executors.newFixedThreadPool(10);

      for ( int i = 0 ; i < item.getTableList().size() ; i++) { 

        Runnable runner = new Importer ( item, i ) ; 
        executor.execute( runner );

      }

      executor.shutdown();

      while ( !executor.isTerminated() ) { 
		  try{
			Thread.sleep(300);
		  }catch(Exception e){
			  logger.fatal(e.getMessage());
		  }
      }

      logger.info("Load Finished !");


    } else if ( args[0].equals("exp") ) { 


      // TODO Auto-generated method stub
      ExporterConfigItem item = new ExporterConfigItem ();
      item.loadJSONFromFile(args[1]);

      ExecutorService executor = Executors.newFixedThreadPool(10);

      for ( int i = 0 ; i < item.getQueryList().size() ; i++) { 


        Runnable runner = new Exporter ( item, i ) ; 
        executor.execute( runner );

      }

      executor.shutdown();

      while ( !executor.isTerminated()) { 
		  try{
			Thread.sleep(300);
		  }catch(Exception e){
			  logger.fatal(e.getMessage());
		  }
      }

      logger.info("Dump Finished !");

    }  else { 
      logger.error ("Invalid option");
    }
  }
}
