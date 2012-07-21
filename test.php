<?php
 //
 // PHP Test script for HyperDex client.
 //
 // Assumes the coordinator is running on port 1234 (as in the simple test in the documentation)
 //
 // To set up the schema for this test run:
 //
 // hyperdex-coordinator-control --host 127.0.0.1 --port 6970 add-space << EOF
 // space datasettest
 // dimensions key, strval, intval (int64), dblval (float), liststrval (list(string)), listintval (list(int64)), listdblval (list(float)), setstrval (set(string)), setintval (set(int64)), setdblval (set(float)), mapstrval (map(string,string)), mapintval (map(string,int64)), mapdblval (map(string,float))
 // key key auto 1 3
 // EOF
 //
 //

  try {

   echo "Connect \n";
    
   $hdp = new hyperclient('127.0.0.1', 1234);

   echo "Put Test\n";

   $val = $hdp->put( 'datasettest','item1', array( 'strval' => 'Brad', 'intval' => 123456 ) );

   var_dump( $val );
   echo "\n\n";


   echo "Get Test\n";

   $val = $hdp->get('datasettest','item1' );
   var_dump( $val );
   echo "\n\n";


   echo "Put Float\n";

   $val = $hdp->put( 'datasettest','item1', array( 'dblval' => 1.234 ) );

   var_dump( $val );
   echo "\n\n";

   echo "Get Float\n";
   $val = $hdp->get( 'datasettest','item1');
   var_dump( $val );
   echo "\n\n";
   

   echo "Put List (string)\n";

   $val = $hdp->put( 'datasettest', 'item1', array( 'liststrval' => array( 'first','second','third' ) ) );
   var_dump( $val );
   echo "\n\n";

   echo "Get List\n";

   $val = $hdp->get('datasettest', 'item1' );
   var_dump( $val );
   echo "\n\n";

   echo "lpush List\n";

   $val = $hdp->lpush( 'datasettest', 'item1', array( 'liststrval' => 'lpush' ) );

   $val = $hdp->get('datasettest', 'item1' );
   var_dump( $val );
   echo "\n\n";


   echo "rpush List\n";

   $val = $hdp->rpush( 'datasettest', 'item1', array( 'liststrval' => 'rpush' ) );

   $val = $hdp->get('datasettest', 'item1' );
   var_dump( $val );
   echo "\n\n";
   
   echo "Put List (int)\n";

   $val = $hdp->put( 'datasettest', 'item1', array( 'listintval' => array( 1,3,5 ) ) );
   var_dump( $val );
   echo "\n\n";

   echo "Get List (int) \n";

   $val = $hdp->get('datasettest', 'item1' );
   var_dump( $val );
   echo "\n\n";
   
   echo "Put List (float)\n";

   $val = $hdp->put( 'datasettest', 'item1', array( 'listdblval' => array( 1.123,3.1415,5.678 ) ) );
   var_dump( $val );
   echo "\n\n";

   echo "Get List (float \n";

   $val = $hdp->get('datasettest', 'item1' );
   var_dump( $val );
   echo "\n\n";

   echo "Put set\n";
   
   $val = $hdp->put( 'datasettest', 'item1', array( 'setstrval' => array( 'one','two','three') ) );

   var_dump( $val );
   echo "\n\n";


   echo "Get Set\n";

   $val = $hdp->get('datasettest', 'item1' );
   var_dump( $val );
   echo "\n\n";
   

   echo "Set union\n";
   
   $val = $hdp->set_union( 'datasettest', 'item1', array( 'setstrval' => array( 'four','five','six') ) );

   var_dump( $val );
   echo "\n\n";

   $val = $hdp->get('datasettest', 'item1' );
   var_dump( $val );
   echo "\n\n";


   echo "Set intersect\n";
   
   $val = $hdp->set_intersect( 'datasettest', 'item1', array( 'setstrval' => array( 'four','five','six') ) );

   var_dump( $val );
   echo "\n\n";

   $val = $hdp->get('datasettest', 'item1' );
   var_dump( $val );
   echo "\n\n";
   
   
   echo "Put map (string/string) \n";

   $val = $hdp->put( 'datasettest', 'item1', array( 'mapstrval' => array( 'first' => 'one', 'second' => 'two', 'third' => 'three') ) );

   var_dump( $val );
   echo "\n\n";


   echo "Get Map (string/string)\n";

   $val = $hdp->get('datasettest', 'item1' );
   var_dump( $val );
   echo "\n\n";
   
   
   echo "Put map (string/int64) \n";

   $val = $hdp->put( 'datasettest', 'item1', array( 'mapintval' => array( 'first' => 1, 'second' => 2, 'third' => 3) ) );

   var_dump( $val );
   echo "\n\n";


   echo "Get Map (string/int64)\n";

   $val = $hdp->get('datasettest', 'item1' );
   var_dump( $val );
   echo "\n\n";
   
   
   echo "Put map (string/double) \n";

   $val = $hdp->put( 'datasettest', 'item1', array( 'mapdblval' => array( 'first' => 1.12, 'second' => 2.23, 'third' => 3.1415927) ) );

   var_dump( $val );
   echo "\n\n";


   echo "Get Map (string/double)\n";

   $val = $hdp->get('datasettest', 'item1' );
   var_dump( $val );
   echo "\n\n";  
   
   
   echo "CondPut test (expect success)\n";

   $val = $hdp->condput( 'datasettest', 'item1', array( 'strval' => 'Brad' ), array( 'strval' => 'chris' ) );

   var_dump( $val );
   echo "\n\n";

   $val = $hdp->get('datasettest', 'item1' );
   var_dump( $val );
   echo "\n\n";  

   
   echo "CondPut test (expect fail)\n";
   
   $val = $hdp->condput( 'datasettest', 'item1', array( 'strval' => 'Brad' ), array( 'strval' => 'chris' ) );

   var_dump( $val );
   echo "\n\n";

   $val = $hdp->get('datasettest', 'item1' );
   var_dump( $val );
   echo "\n\n";  
   
   
   echo "search test - norange (expect success)\n";

   $val = $hdp->search( 'datasettest', array( 'strval' => 'chris' ), array( ) );

   var_dump( $val );
   echo "\n\n";


   echo "search test - norange (expect fail)\n";

   $val = $hdp->search( 'datasettest', array( 'strval' => 'brad' ), array( ) );

   var_dump( $val );
   echo "\n\n";

   
   echo "search test - range (expect success)\n";

   $val = $hdp->search( 'datasettest', array( 'strval' => 'chris' ),array( 'intval' => array(1, 123457) ) );

   var_dump( $val );
   echo "\n\n";


   echo "search test - range (expect fail)\n";

   $val = $hdp->search( 'datasettest', array( 'strval' => 'chris' ), array( 'intval' => array(1, 1234) ) );

   var_dump( $val );
   echo "\n\n";
   
  } catch( Exception $e ) {

    echo "Caught exception " . $e->getMessage() . " ( " . $e->getCode() . " ): " . $e->getTraceAsString() . "\n";  

  }
?>
