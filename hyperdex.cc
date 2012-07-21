/*
  +----------------------------------------------------------------------+
  | HyperDex client PHP plugin                                           |
  +----------------------------------------------------------------------+
  | Copyright (c) 2012 Triton Digital Media                              |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Author: Brad Broerman ( bbroerman@tritonmedia.com )                  |
  +----------------------------------------------------------------------+
 */

/* $Id$ */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "php_hyperdex.h"
#include <unistd.h>
#include <hyperclient.h>
#include <hyperdex.h>
#include <zend_exceptions.h>
#include <zend_operators.h>

ZEND_DECLARE_MODULE_GLOBALS(hyperdex)

/*
 * Class entries for the hyperclient class and the exception
 */
zend_class_entry* cmdex_ce;
zend_class_entry* hyperclient_ce_exception;

zend_object_handlers hyperclient_object_handlers;

/*
 * Helper functions for HyperDex
 */
int hyperdexLoop( hyperclient*  hyperclient, int64_t op_id );

uint64_t byteArrayToUInt64 (unsigned char *arr, size_t bytes);
void uint64ToByteArray (uint64_t num, size_t bytes, unsigned char *arr);

void byteArrayToListString( unsigned char* arr, size_t bytes, zval* return_value );
void byteArrayToListInt64( unsigned char* arr, size_t bytes, zval* return_value );
void byteArrayToListFloat( unsigned char* arr, size_t bytes, zval* return_value );

void byteArrayToMapStringString( unsigned char* arr, size_t bytes, zval* return_value );
void byteArrayToMapStringInt64( unsigned char* arr, size_t bytes, zval* return_value );
void byteArrayToMapStringFloat( unsigned char* arr, size_t bytes, zval* return_value );

void byteArrayToMapInt64String( unsigned char* arr, size_t bytes, zval* return_value );
void byteArrayToMapInt64Int64( unsigned char* arr, size_t bytes, zval* return_value );
void byteArrayToMapInt64Float( unsigned char* arr, size_t bytes, zval* return_value );


void stringListToByteArray( zval* array, unsigned char** output_array, size_t *bytes, int sort );
void intListToByteArray( zval* array, unsigned char** output_array, size_t *bytes, int sort );
void doubleListToByteArray( zval* array, unsigned char** output_array, size_t *bytes, int sort );

void stringInt64HashToByteArray( zval* array, unsigned char** output_array, size_t *bytes );
void stringStringHashToByteArray( zval* array, unsigned char** output_array, size_t *bytes );
void stringDoubleHashToByteArray( zval* array, unsigned char** output_array, size_t *bytes );

void int64Int64HashToByteArray( zval* array, unsigned char** output_array, size_t *bytes );
void int64StringHashToByteArray( zval* array, unsigned char** output_array, size_t *bytes );


void buildAttrFromZval( zval* data, char* arr_key, hyperclient_attribute* attr, enum hyperdatatype expected_type = HYPERDATATYPE_STRING );
void buildRangeFromZval( zval* input, char* arr_key, hyperclient_range_query* rng_q );

void buildArrayFromAttrs( hyperclient_attribute* attr, size_t attrs_sz, zval* data );
void buildZvalFromAttr( hyperclient_attribute* attr, zval* data );

void freeAttrVals( hyperclient_attribute *attr, int len );

int isArrayAllLong( zval* array );
int isArrayAllDouble( zval* array );
int isArrayHashTable( zval* array );

char* HyperDexErrorToMsg( hyperclient_returncode code );

static int php_array_string_compare(const void *a, const void *b TSRMLS_DC);
static int php_array_number_compare(const void *a, const void *b TSRMLS_DC);

/*
 * Allows PHP to store the hyperclient object inbetween calls
 */
struct hyperclient_object {
  zend_object  std;
  hyperclient* hdex = NULL;
};

/* {{{ hyperdex_functions[]
 *
 * Every user visible function must have an entry in hyperdex_functions[].
 */
static zend_function_entry hyperdex_functions[] = {
    PHP_ME(hyperclient, __construct,                NULL, ZEND_ACC_PUBLIC | ZEND_ACC_CTOR )
    PHP_ME(hyperclient, __destruct,                 NULL, ZEND_ACC_PUBLIC | ZEND_ACC_DTOR ) 
    PHP_ME(hyperclient, connect,                 	NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, disconnect,              	NULL, ZEND_ACC_PUBLIC )
    
    PHP_ME(hyperclient, put,                      	NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, put_attr,                 	NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, condput,                  	NULL, ZEND_ACC_PUBLIC )

    PHP_ME(hyperclient, lpush,                    	NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, rpush,                    	NULL, ZEND_ACC_PUBLIC )

    PHP_ME(hyperclient, set_add,                  	NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, set_remove,                	NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, set_union,                 NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, set_intersect,                 NULL, ZEND_ACC_PUBLIC )
    
    PHP_ME(hyperclient, add,                    	NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, sub,                    	NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, mul,                    	NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, div,                    	NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, mod,                    	NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, and,                    	NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, or,                      	NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, xor,                    	NULL, ZEND_ACC_PUBLIC )
    
    PHP_ME(hyperclient, search,                     NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, get,	                    NULL, ZEND_ACC_PUBLIC )
    PHP_ME(hyperclient, get_attr,	                NULL, ZEND_ACC_PUBLIC )
    
    PHP_ME(hyperclient, del,                     	NULL, ZEND_ACC_PUBLIC )	
    PHP_FE_END	/* Must be the last line in hyperdex_functions[] */
};
/* }}} */

/* {{{ hyperdex_module_entry
 */
zend_module_entry hyperdex_module_entry = {
#if ZEND_MODULE_API_NO >= 20010901
    STANDARD_MODULE_HEADER,
#endif
    "hyperdex",
    hyperdex_functions,
    PHP_MINIT(hyperdex),
    PHP_MSHUTDOWN(hyperdex),
    PHP_RINIT(hyperdex),		/* Replace with NULL if there's nothing to do at request start */
    PHP_RSHUTDOWN(hyperdex),	/* Replace with NULL if there's nothing to do at request end */
    PHP_MINFO(hyperdex),
#if ZEND_MODULE_API_NO >= 20010901
    "0.1",                      /* Replace with version number for your extension */
#endif
    STANDARD_MODULE_PROPERTIES
};
/* }}} */

#ifdef COMPILE_DL_HYPERDEX
ZEND_GET_MODULE(hyperdex)
#endif

/* {{{ PHP_INI
 */
PHP_INI_BEGIN()    
PHP_INI_END()
/* }}} */

/* {{{ php_hyperdex_init_globals
 */

static void php_hyperdex_init_globals(zend_hyperdex_globals *hyperdex_globals) {	
 
}
/* }}} */

/* {{{ hyperclient_free_storage     
*/
void hyperclient_free_storage(void *object TSRMLS_DC) {
  hyperclient_object *obj = (hyperclient_object *)object;
  delete obj->hdex;

  zend_hash_destroy( obj->std.properties );
  FREE_HASHTABLE( obj->std.properties );

  efree(obj);
}
/* }}} */

/* {{{ hyperclient_create_handler     
*/
zend_object_value hyperclient_create_handler(zend_class_entry *type TSRMLS_DC) {
  zval *tmp;
  zend_object_value retval;

  hyperclient_object *obj = (hyperclient_object *)emalloc( sizeof( hyperclient_object ) );
  memset( obj, 0, sizeof( hyperclient_object ) );
  obj->std.ce = type;

  ALLOC_HASHTABLE(obj->std.properties);
  zend_hash_init(obj->std.properties, 0, NULL, ZVAL_PTR_DTOR, 0);

#if PHP_VERSION_ID < 50399	
  zend_hash_copy( obj->std.properties, 
                  &type->default_properties,
                  (copy_ctor_func_t)zval_add_ref, (void *)(&tmp),
                  sizeof(zval *) );
#else
  object_properties_init( &(obj->std), type );
#endif

  retval.handle = zend_objects_store_put( obj, NULL, hyperclient_free_storage, NULL TSRMLS_CC );
  retval.handlers = &hyperclient_object_handlers;

  return retval;
}
/* }}} */


PHPAPI zend_class_entry *hyperclient_get_exception_base( ) {
#if (PHP_MAJOR_VERSION == 5) && (PHP_MINOR_VERSION < 2)
  return zend_exception_get_default();
#else
  return zend_exception_get_default(TSRMLS_C);
#endif
}

void hyperclient_init_exception(TSRMLS_D) {
  zend_class_entry e;

  INIT_CLASS_ENTRY(e, "HyperdexException", NULL);
  hyperclient_ce_exception = zend_register_internal_class_ex( &e, (zend_class_entry*)hyperclient_get_exception_base(), NULL TSRMLS_CC );
}


/* {{{ PHP_MINIT_FUNCTION
 */
PHP_MINIT_FUNCTION( hyperdex )
{
  zend_class_entry ce;

  ZEND_INIT_MODULE_GLOBALS( hyperdex, php_hyperdex_init_globals, NULL );

  REGISTER_INI_ENTRIES();

  INIT_CLASS_ENTRY( ce, "hyperclient", hyperdex_functions );
  cmdex_ce = zend_register_internal_class(&ce TSRMLS_CC);

  cmdex_ce->create_object = hyperclient_create_handler;
  memcpy( &hyperclient_object_handlers, zend_get_std_object_handlers(), sizeof( zend_object_handlers ) );
  hyperclient_object_handlers.clone_obj = NULL;

  hyperclient_init_exception( TSRMLS_C );

  return SUCCESS;
}
/* }}} */

/* {{{ PHP_MSHUTDOWN_FUNCTION
 */
PHP_MSHUTDOWN_FUNCTION( hyperdex )
{
  UNREGISTER_INI_ENTRIES();
  return SUCCESS;
}
/* }}} */

/* Remove if there's nothing to do at request start */
/* {{{ PHP_RINIT_FUNCTION
 */
PHP_RINIT_FUNCTION( hyperdex )
{
  return SUCCESS;
}
/* }}} */

/* Remove if there's nothing to do at request end */
/* {{{ PHP_RSHUTDOWN_FUNCTION
 */
PHP_RSHUTDOWN_FUNCTION( hyperdex )
{
  return SUCCESS;
}
/* }}} */

/* {{{ PHP_MINFO_FUNCTION
 */
PHP_MINFO_FUNCTION( hyperdex )
{
  php_info_print_table_start();
  php_info_print_table_header( 2, "hyperdex support", "enabled" );
  php_info_print_table_end();

  /* Remove comments if you have entries in php.ini
	DISPLAY_INI_ENTRIES();
   */
}
/* }}} */


/* {{{ proto __construct(string host, int port)
   Construct a HyperDex Client instance, and connect to a server. */
PHP_METHOD( hyperclient, __construct ) {
  hyperclient*  hdex        = NULL;
  char*         host        = NULL;
  int           host_len    = -1;
  long          port        = 0;
  zval*         object      = getThis();

  // Get the host name / IP and the port number.
  if (zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "sl", &host, &host_len, &port ) == FAILURE ) {
    RETURN_NULL();
  }
  
  // Validate them, and throw an exception if there's an issue.
  if( NULL == host || 0 >= host_len ) {
    zend_throw_exception( hyperclient_ce_exception, "Invalid host", 1001 TSRMLS_CC );
  }

  if( 0 >= port || 65536 <= port ) {
    zend_throw_exception( hyperclient_ce_exception, "Invalid port", 1001 TSRMLS_CC );
  }

  // Now try to create the hyperclient (and connect). Throw an exception if there are errors.
  try {
    hdex = new hyperclient( host, port );
    if( NULL == hdex ) {
      zend_throw_exception( hyperclient_ce_exception, "Unable to connect to HyperDex",1 TSRMLS_CC );
    }
  } catch( ... ) {
    hdex = NULL;
    zend_throw_exception( hyperclient_ce_exception, "Unable to connect to HyperDex",1 TSRMLS_CC );
  }
  
  // If all is good, then set the PHP thread's storage object
  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object(object TSRMLS_CC );
  obj->hdex = hdex;

}
/* }}} */



/* {{{ proto Boolean __destruct( )
   Disconnect from the HyperDex server, and clean upallocated memory */
PHP_METHOD(hyperclient, __destruct) {
}
/* }}} *


/* {{{ proto Boolean connect(string host, int port)
   Connect to a HyperDex server */
PHP_METHOD( hyperclient, connect ) {
  hyperclient*  hdex        = NULL;
  char*         host        = NULL;
  int           host_len    = -1;
  long          port        = 0;

  // Get the host name / IP and the port number.
  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "sl", &host, &host_len, &port ) == FAILURE ) {
    RETURN_FALSE;
  }

  // Validate them, and throw an exception if there's an issue.
  if( NULL == host || 0 >= host_len ) {
    zend_throw_exception( hyperclient_ce_exception, "Invalid host", 1001 TSRMLS_CC );
    RETURN_FALSE;
  }

  if( 0 >= port || 65536 <= port ) {
    zend_throw_exception( hyperclient_ce_exception, "Invalid port", 1001 TSRMLS_CC );
    RETURN_FALSE;
  }
  
  // Get the hyperclient object from PHP's thread storeage.
  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  // If we get it, we need to delete the old one and create the new one.
  if( hdex != NULL ) {
    delete hdex;

    try {
    
      // Instantiate the hyperclient, and try to connect.
      hdex = new hyperclient( host, port );
      
      if( NULL == hdex ) {
        zend_throw_exception(hyperclient_ce_exception, "Unable to connect to HyperDex",1 TSRMLS_CC);
      }
      
    } catch( ... ) {
      hdex = NULL;
      zend_throw_exception(hyperclient_ce_exception, "Unable to connect to HyperDex",1 TSRMLS_CC);
    }

    // Push it back to the storage object, and return success.
    obj->hdex = hdex;
  }

  // Return success if we set a valid hyperclient 
  RETURN_BOOL( hdex != NULL );
}
/* }}} */    

/* {{{ proto Boolean disconnect( )
   Disconnect from the HyperDex server */
PHP_METHOD( hyperclient, disconnect ) {
  hyperclient*        hdex = NULL;  
  hyperclient_object* obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  
  hdex = obj->hdex;

  if( NULL != hdex ) {
    delete hdex;
    obj->hdex = NULL;
    RETURN_TRUE;
  }

  RETURN_FALSE;
}
/* }}} */

/* {{{ proto Boolean put(string space, String key, Array attributes)
   Sets one or more attributes for a given space and key. */
PHP_METHOD( hyperclient, put ) {
  hyperclient*            hdex        = NULL;
  char*                   scope       = NULL;
  int                     scope_len   = -1;
  char*                   key         = NULL;
  int                     key_len     = -1;
  
  zval                    *arr        = NULL;
  zval                    **data      = NULL;   
  HashTable               *arr_hash   = NULL;
  HashPosition            pointer;  
  
  hyperclient_attribute*  attr        = NULL;
  int                     attr_cnt    = 0;

  // Get the input parameters: Scope, Key, and Attrs.
  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssa", &scope, &scope_len, &key, &key_len, &arr ) == FAILURE ) {
    RETURN_FALSE;
  }
  
  // Set our initial return value to false. Will be set to true if successful.
  RETVAL_TRUE;

  // Get the hyperclient object from PHP's thread storeage.
  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  // If we have one, perform the operation.
  if( NULL != hdex ) {
    try {

      // Get the array hash for the attributes to be put.
      arr_hash = Z_ARRVAL_P( arr );
      
      // Allocate the HyperDex attribute array 
      attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];
      
      // Convert each entry (attribute) in the input array into an element in the hyperclient_attribute array.
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;
         
        // Only take attributes (make sure the hash key is a string)
        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
          
          // Get the expected type (we may need it)
          hyperclient_returncode op_status;
          enum hyperdatatype expected_type = hdex->attribute_type( (const char*)scope, 
                                                                   (const char*)arr_key,
                                                                   &op_status );
                                                                   
          // And convrt the hash value's ZVAL into the Attribute.
          buildAttrFromZval( *data, arr_key, &attr[attr_cnt], expected_type );
          attr_cnt++;
        }
      }

      // Now, call the HyperClient to do the put of the attribute array.
      hyperclient_returncode op_status;

      int64_t op_id = hdex->put( (const char*)scope,   /* the scope (table) */
                                 (const char*)key,     /* key (treated as opaque bytes) */
                                 (size_t)key_len,       /* key size (in bytes) */ 
                                 attr,                  /* the attributes / values to store. */
                                 attr_cnt,              /* the number of attributes */
                                 &op_status );          /* where to put the status of the op */

      // Check the return code. Throw an exception and return false if there is an error.
      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }
      
    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "Set failed", 1 TSRMLS_CC );
    }
  }
  
  // Clean up allocated data 
  if( NULL != attr ) {
    freeAttrVals( attr, attr_cnt );
    delete[] attr;  
  }

  // And finally return.
  return;
}
/* }}} */

/* {{{ proto Boolean put_attr(string space, string key, string attribute, Mixed value )
       Sets a given attribute for a given record (as defined by key) for a given space. */
PHP_METHOD( hyperclient, put_attr ) {
  hyperclient*            hdex          = NULL;
  char*                   scope         = NULL;
  int                     scope_len     = -1;
  char*                   key           = NULL;
  int                     key_len       = -1;
  char*                   attr_name     = NULL;
  int                     attr_name_len = -1;  
  
  zval*                   val           = NULL;  
  
  hyperclient_attribute*  attr          = NULL;

  // Get the input parameters: Scope, Key, attr name, the value to put there.
  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sssz", &scope, &scope_len, &key, &key_len, &attr_name, &attr_name_len, &val) == FAILURE) {
    RETURN_FALSE;
  }

  // Get the hyperclient object from PHP's thread storeage.
  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC);
  hdex = obj->hdex;
  
  // Set our intial return value to false (it will be set to true if the operation was successful)
  RETVAL_FALSE;
  
  // If we have one, perform the operation.
  if( NULL != hdex ) {
    try {
    
      // Get the expected type (we may need it)
      hyperclient_returncode op_status;
      enum hyperdatatype expected_type = hdex->attribute_type( (const char*)scope, 
                                                               (const char*)attr_name,
                                                               &op_status );
                                                               
      // And convert the ZVAL into the hyperclient_attribute.                                                               
      attr  = new hyperclient_attribute();      
      buildAttrFromZval( val, attr_name, attr, expected_type );      
      
      // Now, call the HyperClient to do the put of the attribute array.
      int64_t op_id = hdex->put( (const char*)scope,   /* the scope (table) */
                                 (const char*)key,     /* key (treated as opaque bytes) */
                                 (size_t)key_len,       /* key size (in bytes) */ 
                                 attr,                  /* the attributes / values to store. */
                                 1,                     /* the number of attributes */
                                 &op_status );          /* where to put the status of the op */          

      // Check the return code. Throw an exception and return false if there is an error.
      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }

    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "Set_attr failed", 1 TSRMLS_CC );
    }
  }

  // Clean up allocated memeory
  if( NULL != attr ) {
    freeAttrVals( attr, 1 );
    delete attr;  
  }
  
  // and return.
  return;
}
/* }}} */


/* {{{ proto Boolean condput(string space, string key, Array conditionals, Array attributes)
   Update the attributes for a key if and only if the existing attribute values match the conditionals */
PHP_METHOD( hyperclient, condput ) {
  hyperclient*            hdex          = NULL;
  char*                   scope         = NULL;
  int                     scope_len     = -1;
  char*                   key           = NULL;
  int                     key_len       = -1;
  
  zval                    *cond         = NULL;
  zval                    *attr         = NULL;  
  zval                    **data        = NULL;
  HashTable               *arr_hash     = NULL;
  HashPosition            pointer;
  
  hyperclient_attribute*  cond_attr     = NULL;
  int                     cond_attr_cnt = 0;
  
  hyperclient_attribute*  val_attr      = NULL;
  int                     val_attr_cnt  = 0;
  
  int                     return_code   = 0;

  // Get the input parameters: Scope, Key, list of precondidtions (array) , the values to put (array).
  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssaa", &scope, &scope_len, &key, &key_len, &cond, &attr ) == FAILURE ) {
    RETURN_FALSE;
  }

  // Get the hyperclient object from PHP's thread storeage.
  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  // If we have one, perform the operation.
  if( NULL != hdex ) {
    try {
      
      // Start by iterating through the conditional array, and building a list of hyperclient_attributes from the zvals.
      arr_hash = Z_ARRVAL_P(cond);

      cond_attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];
      cond_attr_cnt = 0;
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;

        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING ) {
          hyperclient_returncode op_status;
          enum hyperdatatype expected_type = hdex->attribute_type( (const char*)scope, 
                                                                   (const char*)arr_key,
                                                                   &op_status );

          buildAttrFromZval( *data, arr_key, &cond_attr[cond_attr_cnt], expected_type );       
          cond_attr_cnt++;
        }
      }

      // Then iterate across the attr array, and build a list of hyperclient_attributes from the zvals.      
      arr_hash = Z_ARRVAL_P(attr);

      val_attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];      
      val_attr_cnt = 0;
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;

        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING ) {
          hyperclient_returncode op_status;
          enum hyperdatatype expected_type = hdex->attribute_type( (const char*)scope, 
                                                                   (const char*)arr_key,
                                                                   &op_status );

          buildAttrFromZval( *data, arr_key, &val_attr[val_attr_cnt], expected_type );
          val_attr_cnt++;
        }
      }                 


      // Now, perform the operation
      hyperclient_returncode op_status;

      int64_t op_id = hdex->condput( (const char*)scope,    /* the scope (table) */
                                     (const char*)key,      /* key (treated as opaque bytes) */
                                     (size_t)key_len,        /* key size (in bytes) */ 
                                     cond_attr,              /* the conditional attributes / values test for. */
                                     cond_attr_cnt,          /* the number of conditional attributes */
                                     val_attr,               /* the attributes / values to store. */
                                     val_attr_cnt,           /* the number of attributes to store */
                                     &op_status );           /* where to put the status of the op */ 
                                 
      // and check for errors / exception.
      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
        
          // Only return TRUE if the operation completed successfully. 
          if( op_status == HYPERCLIENT_SUCCESS ) {
            return_code = 1;
          }
          
        }
        
      }           

    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "Set failed", 1 TSRMLS_CC );
    }
  }

  // Free any allocated memory.
  if( NULL != cond_attr ) {
    freeAttrVals( cond_attr, cond_attr_cnt );
    delete[] cond_attr;
  }
  
  if( NULL != val_attr ) {
    freeAttrVals( val_attr, val_attr_cnt );
    delete[] val_attr;
  }

  // And return success / fail.
  RETURN_BOOL( return_code );
}
/* }}} */


/* {{{ proto Boolean lpush(String space, String key, Array attributes)
   Pushes the specified object to the head of the list of each attribute specified by the request.  */
PHP_METHOD( hyperclient, lpush ) {
  hyperclient*            hdex        = NULL;
  char*                   scope       = NULL;
  int                     scope_len   = -1;
  char*                   key         = NULL;
  int                     key_len     = -1;
  
  zval                    *arr        = NULL;
  zval                    **data      = NULL;   
  HashTable               *arr_hash   = NULL;
  HashPosition            pointer;  
  
  hyperclient_attribute*  attr        = NULL;
  int                     attr_cnt    = 0;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssa", &scope, &scope_len, &key, &key_len, &arr ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  RETVAL_FALSE;
  
  if( NULL != hdex ) {
    try {

      arr_hash = Z_ARRVAL_P(arr);
      
      attr = new hyperclient_attribute[zend_hash_num_elements(arr_hash)];
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;
         
        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING ) {
          buildAttrFromZval( *data, arr_key, &attr[attr_cnt] );
          attr_cnt++;
        }
      }

      hyperclient_returncode op_status;

      int64_t op_id = hdex->list_lpush ( (const char*)scope,   /* the scope (table) */
                                         (const char*)key,     /* key (treated as opaque bytes) */
                                         (size_t)key_len,       /* key size (in bytes) */ 
                                         attr,                  /* the attributes / values to store. */
                                         attr_cnt,              /* the number of attributes */
                                         &op_status );          /* where to put the status of the op */

      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }
    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "lpush failed", 1 TSRMLS_CC );
    }
  }
  
  if( NULL != attr ) {
    freeAttrVals( attr, attr_cnt );
    delete[] attr;  
  }

  return;
}
/* }}} */


/* {{{ proto Boolean rpush(String space, String key, Array attributes)
   Pushes the specified object to the tail of the list of each attribute specified by the request.  */
PHP_METHOD(hyperclient,rpush) {
  hyperclient*            hdex        = NULL;
  char*                   scope       = NULL;
  int                     scope_len   = -1;
  char*                   key         = NULL;
  int                     key_len     = -1;
  
  zval                    *arr        = NULL;
  zval                    **data      = NULL;   
  HashTable               *arr_hash   = NULL;
  HashPosition            pointer;  
  
  hyperclient_attribute*  attr        = NULL;
  int                     attr_cnt    = 0;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssa", &scope, &scope_len, &key, &key_len, &arr ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  RETVAL_FALSE;
  
  if( NULL != hdex ) {
    try {

      arr_hash = Z_ARRVAL_P(arr);
      
      attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;
         
        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING ) {
          buildAttrFromZval( *data, arr_key, &attr[attr_cnt] );
          attr_cnt++;
        }
      }

      hyperclient_returncode op_status;

      int64_t op_id = hdex->list_rpush( (const char*)scope,   /* the scope (table) */
                                        (const char*)key,     /* key (treated as opaque bytes) */
                                        (size_t)key_len,       /* key size (in bytes) */ 
                                        attr,                  /* the attributes / values to store. */
                                        attr_cnt,              /* the number of attributes */
                                        &op_status );          /* where to put the status of the op */

      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }

    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "lpush failed", 1 TSRMLS_CC );
    }
  }
  
  if( NULL != attr ) {
    freeAttrVals( attr, attr_cnt );
    delete[] attr;  
  }

  return;
}
/* }}} */

/* {{{ proto Boolean set_add(string space, String key, Array attributes)
   Adds a value to one or more sets for a given space and key if the value is not already in the set. */
PHP_METHOD( hyperclient, set_add ) {
  hyperclient*            hdex        = NULL;
  char*                   scope       = NULL;
  int                     scope_len   = -1;
  char*                   key         = NULL;
  int                     key_len     = -1;
  
  zval                    *arr        = NULL;
  zval                    **data      = NULL;   
  HashTable               *arr_hash   = NULL;
  HashPosition            pointer;  
  
  hyperclient_attribute*  attr        = NULL;
  int                     attr_cnt    = 0;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssa", &scope, &scope_len, &key, &key_len, &arr ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  RETVAL_FALSE;
  
  if( NULL != hdex ) {
    try {

      arr_hash = Z_ARRVAL_P( arr );
      
      attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;
         
        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {        
          buildAttrFromZval( *data, arr_key, &attr[attr_cnt] );
          attr_cnt++;
        }
      }

      hyperclient_returncode op_status;

      int64_t op_id = hdex->set_add( (const char*)scope,   /* the scope (table) */
                                     (const char*)key,     /* key (treated as opaque bytes) */
                                     (size_t)key_len,       /* key size (in bytes) */ 
                                     attr,                  /* the attributes / values to store. */
                                     attr_cnt,              /* the number of attributes */
                                     &op_status );          /* where to put the status of the op */

      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }
    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "set_add failed", 1 TSRMLS_CC );
    }
  }
  
  if( NULL != attr ) {
    freeAttrVals( attr, attr_cnt );
    delete[] attr;  
  }

  return;
}
/* }}} */


/* {{{ proto Boolean set_remove(string space, String key, Array attributes)
   Removes a value to one or more sets for a given space and key if the value is in the set. */
PHP_METHOD( hyperclient, set_remove ) {
  hyperclient*            hdex        = NULL;
  char*                   scope       = NULL;
  int                     scope_len   = -1;
  char*                   key         = NULL;
  int                     key_len     = -1;
  
  zval                    *arr        = NULL;
  zval                    **data      = NULL;   
  HashTable               *arr_hash   = NULL;
  HashPosition            pointer;  
  
  hyperclient_attribute*  attr        = NULL;
  int                     attr_cnt    = 0;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssa", &scope, &scope_len, &key, &key_len, &arr ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  RETVAL_FALSE;
  
  if( NULL != hdex ) {
    try {

      arr_hash = Z_ARRVAL_P( arr );
      
      attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;
         
        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
          buildAttrFromZval( *data, arr_key, &attr[attr_cnt] );
          attr_cnt++;
        }
      }

      hyperclient_returncode op_status;

      int64_t op_id = hdex->set_remove( (const char*)scope,   /* the scope (table) */
                                        (const char*)key,     /* key (treated as opaque bytes) */
                                        (size_t)key_len,       /* key size (in bytes) */ 
                                        attr,                  /* the attributes / values to store. */
                                        attr_cnt,              /* the number of attributes */
                                        &op_status );          /* where to put the status of the op */

      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }

    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "set_add failed", 1 TSRMLS_CC );
    }
  }
  
  if( NULL != attr ) {
    freeAttrVals( attr, attr_cnt );
    delete[] attr;  
  }

  return;
}
/* }}} */


/* {{{ proto Boolean set_union(string space, String key, Array attributes)
   Performs a set union with one or more sets for a given space and key. Sets will be defined by the
   key of the passed in array, with the set to be unioned with the stored set passed as the value.
   The union will be atomic without interference from other operations. */
PHP_METHOD( hyperclient, set_union ) {
  hyperclient*            hdex        = NULL;
  char*                   scope       = NULL;
  int                     scope_len   = -1;
  char*                   key         = NULL;
  int                     key_len     = -1;

  zval                    *arr        = NULL;
  zval                    **data      = NULL;
  HashTable               *arr_hash   = NULL;
  HashPosition            pointer;

  hyperclient_attribute*  attr        = NULL;
  int                     attr_cnt    = 0;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssa", &scope, &scope_len, &key, &key_len, &arr ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;

  RETVAL_FALSE;

  if( NULL != hdex ) {
    try {

      arr_hash = Z_ARRVAL_P( arr );

      attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];

      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;

        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
          hyperclient_returncode op_status;
          enum hyperdatatype expected_type = hdex->attribute_type( (const char*)scope, 
                                                                   (const char*)arr_key,
                                                                   &op_status );
                                                                         
          buildAttrFromZval( *data, arr_key, &attr[attr_cnt], expected_type );
          attr_cnt++;
        }
      }
      
      hyperclient_returncode op_status;

      int64_t op_id = hdex->set_union( (const char*)scope,   /* the scope (table) */
                                       (const char*)key,     /* key (treated as opaque bytes) */
                                       (size_t)key_len,       /* key size (in bytes) */
                                       attr,                  /* the attributes / values to store. */
                                       attr_cnt,              /* the number of attributes */
                                       &op_status );          /* where to put the status of the op */

      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }

    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "set_add failed", 1 TSRMLS_CC );
    }
  }

  if( NULL != attr ) {
    freeAttrVals( attr, attr_cnt );
    delete[] attr;
  }

  return;
}
/* }}} */


/* {{{ proto Boolean set_intersect(string space, String key, Array attributes)
   Performs a set intersection with one or more sets for a given space and key. Sets will be defined by the
   key of the passed in array, with the set to be unioned with the stored set passed as the value.
   The union will be atomic without interference from other operations. */
PHP_METHOD( hyperclient, set_intersect ) {
  hyperclient*            hdex        = NULL;
  char*                   scope       = NULL;
  int                     scope_len   = -1;
  char*                   key         = NULL;
  int                     key_len     = -1;

  zval                    *arr        = NULL;
  zval                    **data      = NULL;
  HashTable               *arr_hash   = NULL;
  HashPosition            pointer;

  hyperclient_attribute*  attr        = NULL;
  int                     attr_cnt    = 0;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssa", &scope, &scope_len, &key, &key_len, &arr ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  RETVAL_FALSE;

  if( NULL != hdex ) {
    try {

      arr_hash = Z_ARRVAL_P( arr );

      attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];

      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;

        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
          hyperclient_returncode op_status;
          enum hyperdatatype expected_type = hdex->attribute_type( (const char*)scope, 
                                                                   (const char*)arr_key,
                                                                   &op_status );
                                                                           
          buildAttrFromZval( *data, arr_key, &attr[attr_cnt], expected_type );
          attr_cnt++;
        }
      }

      hyperclient_returncode op_status;

      int64_t op_id = hdex->set_intersect( (const char*)scope,   /* the scope (table) */
                                           (const char*)key,     /* key (treated as opaque bytes) */
                                           (size_t)key_len,       /* key size (in bytes) */
                                           attr,                  /* the attributes / values to store. */
                                           attr_cnt,              /* the number of attributes */
                                           &op_status );          /* where to put the status of the op */

      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }

    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "set_add failed", 1 TSRMLS_CC );
    }
  }

  if( NULL != attr ) {
    freeAttrVals( attr, attr_cnt );
    delete[] attr;
  }

  return;
}
/* }}} */


/* {{{ proto Boolean add(string space, String key, Array attributes)
   Atomically adds integers to one or more attributes for a given space and key. */
PHP_METHOD( hyperclient, add ) {
  hyperclient*            hdex        = NULL;
  char*                   scope       = NULL;
  int                     scope_len   = -1;
  char*                   key         = NULL;
  int                     key_len     = -1;
  
  zval                    *arr        = NULL;
  zval                    **data      = NULL;   
  HashTable               *arr_hash   = NULL;
  HashPosition            pointer;  
  
  hyperclient_attribute*  attr        = NULL;
  int                     attr_cnt    = 0;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssa", &scope, &scope_len, &key, &key_len, &arr ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  RETVAL_FALSE;
  
  if( NULL != hdex ) {
    try {

      arr_hash = Z_ARRVAL_P( arr );
      
      attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;
         
        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
          buildAttrFromZval( *data, arr_key, &attr[attr_cnt] );
          attr_cnt++;
        }
      }

      hyperclient_returncode op_status;

      int64_t op_id = hdex->atomic_add( (const char*)scope,   /* the scope (table) */
                                        (const char*)key,     /* key (treated as opaque bytes) */
                                        (size_t)key_len,       /* key size (in bytes) */ 
                                        attr,                  /* the attributes / values to store. */
                                        attr_cnt,              /* the number of attributes */
                                        &op_status);           /* where to put the status of the op */

      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }

    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "add failed", 1 TSRMLS_CC );
    }
  }
  
  if( NULL != attr ) {
    freeAttrVals( attr, attr_cnt );
    delete[] attr;  
  }

  return;
}
/* }}} */


/* {{{ proto Boolean sub(string space, String key, Array attributes)
   Atomically subtracts integers to one or more attributes for a given space and key. */
PHP_METHOD( hyperclient, sub ) {
  hyperclient*            hdex        = NULL;
  char*                   scope       = NULL;
  int                     scope_len   = -1;
  char*                   key         = NULL;
  int                     key_len     = -1;
  
  zval                    *arr        = NULL;
  zval                    **data      = NULL;   
  HashTable               *arr_hash   = NULL;
  HashPosition            pointer;  
  
  hyperclient_attribute*  attr        = NULL;
  int                     attr_cnt    = 0;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssa", &scope, &scope_len, &key, &key_len, &arr ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  RETVAL_FALSE;
  
  if( NULL != hdex ) {
    try {

      arr_hash = Z_ARRVAL_P( arr );
      
      attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;
         
        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
          buildAttrFromZval( *data, arr_key, &attr[attr_cnt] );
          attr_cnt++;
        }
      }

      hyperclient_returncode op_status;

      int64_t op_id = hdex->atomic_sub( (const char*)scope,   /* the scope (table) */
                                        (const char*)key,     /* key (treated as opaque bytes) */
                                        (size_t)key_len,       /* key size (in bytes) */ 
                                        attr,                  /* the attributes / values to store. */
                                        attr_cnt,              /* the number of attributes */
                                        &op_status);           /* where to put the status of the op */

      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }

    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "sub failed", 1 TSRMLS_CC );
    }
  }
  
  if( NULL != attr ) {
    freeAttrVals( attr, attr_cnt );
    delete[] attr;  
  }

  return;
}
/* }}} */


/* {{{ proto Boolean mul(string space, String key, Array attributes)
   Atomically multiplies integers to one or more attributes for a given space and key. */
PHP_METHOD( hyperclient, mul ) {
  hyperclient*            hdex        = NULL;
  char*                   scope       = NULL;
  int                     scope_len   = -1;
  char*                   key         = NULL;
  int                     key_len     = -1;
  
  zval                    *arr        = NULL;
  zval                    **data      = NULL;   
  HashTable               *arr_hash   = NULL;
  HashPosition            pointer;  
  
  hyperclient_attribute*  attr        = NULL;
  int                     attr_cnt    = 0;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssa", &scope, &scope_len, &key, &key_len, &arr ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  RETVAL_FALSE;
  
  if( NULL != hdex ) {
    try {

      arr_hash = Z_ARRVAL_P( arr );
      
      attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;
         
        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
          buildAttrFromZval( *data, arr_key, &attr[attr_cnt] );
          attr_cnt++;
        }
      }

      hyperclient_returncode op_status;

      int64_t op_id = hdex->atomic_mul( (const char*)scope,   /* the scope (table) */
                                        (const char*)key,     /* key (treated as opaque bytes) */
                                        (size_t)key_len,       /* key size (in bytes) */ 
                                        attr,                  /* the attributes / values to store. */
                                        attr_cnt,              /* the number of attributes */
                                        &op_status);           /* where to put the status of the op */

      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }

    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "mul failed", 1 TSRMLS_CC );
    }
  }
  
  if( NULL != attr ) {
    freeAttrVals( attr, attr_cnt );
    delete[] attr;  
  }

  return;
}
/* }}} */


/* {{{ proto Boolean div(string space, String key, Array attributes)
   Atomically divides the values of one or more attributes for a given space and key by the 
   divisor in the input array for that attribute. */
PHP_METHOD( hyperclient, div ) {
  hyperclient*            hdex        = NULL;
  char*                   scope       = NULL;
  int                     scope_len   = -1;
  char*                   key         = NULL;
  int                     key_len     = -1;
  
  zval                    *arr        = NULL;
  zval                    **data      = NULL;   
  HashTable               *arr_hash   = NULL;
  HashPosition            pointer;  
  
  hyperclient_attribute*  attr        = NULL;
  int                     attr_cnt    = 0;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssa", &scope, &scope_len, &key, &key_len, &arr ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  RETVAL_FALSE;
  
  if( NULL != hdex ) {
    try {

      arr_hash = Z_ARRVAL_P( arr );
      
      attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;
         
        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
          buildAttrFromZval( *data, arr_key, &attr[attr_cnt] );
          attr_cnt++;
        }
      }

      hyperclient_returncode op_status;

      int64_t op_id = hdex->atomic_div( (const char*)scope,   /* the scope (table) */
                                        (const char*)key,     /* key (treated as opaque bytes) */
                                        (size_t)key_len,       /* key size (in bytes) */ 
                                        attr,                  /* the attributes / values to store. */
                                        attr_cnt,              /* the number of attributes */
                                        &op_status);           /* where to put the status of the op */

      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }

    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "mul failed", 1 TSRMLS_CC );
    }
  }
  
  if( NULL != attr ) {
    freeAttrVals( attr, attr_cnt );
    delete[] attr;  
  }

  return;
}
/* }}} */


/* {{{ proto Boolean mod(string space, String key, Array attributes)
   Atomically divides the values of one or more attributes for a given space and key by the 
   divisor in the input array for that attribute, and stores the remainder. */
PHP_METHOD( hyperclient, mod ) {
  hyperclient*            hdex        = NULL;
  char*                   scope       = NULL;
  int                     scope_len   = -1;
  char*                   key         = NULL;
  int                     key_len     = -1;
  
  zval                    *arr        = NULL;
  zval                    **data      = NULL;   
  HashTable               *arr_hash   = NULL;
  HashPosition            pointer;  
  
  hyperclient_attribute*  attr        = NULL;
  int                     attr_cnt    = 0;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssa", &scope, &scope_len, &key, &key_len, &arr ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  RETVAL_FALSE;
  
  if( NULL != hdex ) {
    try {

      arr_hash = Z_ARRVAL_P( arr );
      
      attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;
         
        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
          buildAttrFromZval( *data, arr_key, &attr[attr_cnt] );
          attr_cnt++;
        }
      }

      hyperclient_returncode op_status;

      int64_t op_id = hdex->atomic_mod( (const char*)scope,   /* the scope (table) */
                                        (const char*)key,     /* key (treated as opaque bytes) */
                                        (size_t)key_len,       /* key size (in bytes) */ 
                                        attr,                  /* the attributes / values to store. */
                                        attr_cnt,              /* the number of attributes */
                                        &op_status);           /* where to put the status of the op */

      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }

    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "mul failed", 1 TSRMLS_CC );
    }
  }
  
  if( NULL != attr ) {
    freeAttrVals( attr, attr_cnt );
    delete[] attr;  
  }

  return;
}
/* }}} */


/* {{{ proto Boolean and(string space, String key, Array attributes)
   Atomically stores the bitwise AND of one or more attributes for a given space and key and 
   the integer passed in the input array for that attribute. */
PHP_METHOD( hyperclient, and ) {
  hyperclient*            hdex        = NULL;
  char*                   scope       = NULL;
  int                     scope_len   = -1;
  char*                   key         = NULL;
  int                     key_len     = -1;
  
  zval                    *arr        = NULL;
  zval                    **data      = NULL;   
  HashTable               *arr_hash   = NULL;
  HashPosition            pointer;  
  
  hyperclient_attribute*  attr        = NULL;
  int                     attr_cnt    = 0;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssa", &scope, &scope_len, &key, &key_len, &arr ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  RETVAL_FALSE;
  
  if( NULL != hdex ) {
    try {

      arr_hash = Z_ARRVAL_P( arr );
      
      attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;
         
        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
          buildAttrFromZval( *data, arr_key, &attr[attr_cnt] );
          attr_cnt++;
        }
      }

      hyperclient_returncode op_status;

      int64_t op_id = hdex->atomic_and( (const char*)scope,   /* the scope (table) */
                                        (const char*)key,     /* key (treated as opaque bytes) */
                                        (size_t)key_len,       /* key size (in bytes) */ 
                                        attr,                  /* the attributes / values to store. */
                                        attr_cnt,              /* the number of attributes */
                                        &op_status);           /* where to put the status of the op */

      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }

    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "mul failed", 1 TSRMLS_CC );
    }
  }
  
  if( NULL != attr ) {
    freeAttrVals( attr, attr_cnt );
    delete[] attr;  
  }

  return;
}
/* }}} */


/* {{{ proto Boolean or(string space, String key, Array attributes)
   Atomically stores the bitwise OR of one or more attributes for a given space and key and the 
   integer passed in the input array for that attribute. */
PHP_METHOD( hyperclient, or ) {
  hyperclient*            hdex        = NULL;
  char*                   scope       = NULL;
  int                     scope_len   = -1;
  char*                   key         = NULL;
  int                     key_len     = -1;
  
  zval                    *arr        = NULL;
  zval                    **data      = NULL;   
  HashTable               *arr_hash   = NULL;
  HashPosition            pointer;  
  
  hyperclient_attribute*  attr        = NULL;
  int                     attr_cnt    = 0;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssa", &scope, &scope_len, &key, &key_len, &arr ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  RETVAL_FALSE;
  
  if( NULL != hdex ) {
    try {

      arr_hash = Z_ARRVAL_P( arr );
      
      attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;
         
        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
          buildAttrFromZval( *data, arr_key, &attr[attr_cnt] );
          attr_cnt++;
        }
      }

      hyperclient_returncode op_status;

      int64_t op_id = hdex->atomic_or( (const char*)scope,   /* the scope (table) */
                                       (const char*)key,     /* key (treated as opaque bytes) */
                                       (size_t)key_len,       /* key size (in bytes) */ 
                                       attr,                  /* the attributes / values to store. */
                                       attr_cnt,              /* the number of attributes */
                                       &op_status);           /* where to put the status of the op */

      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }

    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "mul failed", 1 TSRMLS_CC );
    }
  }
  
  if( NULL != attr ) {
    freeAttrVals( attr, attr_cnt );
    delete[] attr;  
  }

  return;
}
/* }}} */


/* {{{ proto Boolean xor(string space, String key, Array attributes)
   Atomically stores the bitwise XOR (Exclusive OR of one or more attributes for a given space and key and 
   the integer passed in the input array for that attribute. */
PHP_METHOD( hyperclient, xor ) {
  hyperclient*            hdex        = NULL;
  char*                   scope       = NULL;
  int                     scope_len   = -1;
  char*                   key         = NULL;
  int                     key_len     = -1;
  
  zval                    *arr        = NULL;
  zval                    **data      = NULL;   
  HashTable               *arr_hash   = NULL;
  HashPosition            pointer;  
  
  hyperclient_attribute*  attr        = NULL;
  int                     attr_cnt    = 0;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ssa", &scope, &scope_len, &key, &key_len, &arr ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  RETVAL_FALSE;
  
  if( NULL != hdex ) {
    try {

      arr_hash = Z_ARRVAL_P( arr );
      
      attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;
         
        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
          buildAttrFromZval( *data, arr_key, &attr[attr_cnt] );
          attr_cnt++;
        }
      }

      hyperclient_returncode op_status;

      int64_t op_id = hdex->atomic_xor( (const char*)scope,   /* the scope (table) */
                                        (const char*)key,     /* key (treated as opaque bytes) */
                                        (size_t)key_len,       /* key size (in bytes) */ 
                                        attr,                  /* the attributes / values to store. */
                                        attr_cnt,              /* the number of attributes */
                                        &op_status);           /* where to put the status of the op */

      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
        if( 0 < hyperdexLoop( hdex, op_id ) ) {
          RETVAL_TRUE;
        }
      }

    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "mul failed", 1 TSRMLS_CC );
    }
  }
  
  if( NULL != attr ) {
    freeAttrVals( attr, attr_cnt );
    delete[] attr;  
  }

  return;
}
/* }}} */


/* {{{ proto Mixed search(string space, Array equality_conditionals, Array range_conditionals)
   Searches for records with attributes matching the equality_conditionals and the range_conditionals. 
   Returns an array of the matching records. */
PHP_METHOD( hyperclient, search ) {
  hyperclient*             hdex         = NULL;
  char*                    scope        = NULL;
  int                      scope_len    = -1;
  zval*                    eq_cond      = NULL;
  zval*                    range_cond   = NULL;
  
  zval**                   data;
  HashTable*               arr_hash;
  HashPosition             pointer;
  
  hyperclient_attribute*   eq_cond_attr = NULL;
  hyperclient_range_query* rng_cond_qry = NULL;
  int                      eq_attr_cnt  = 0;
  int                      rng_attr_cnt = 0;

  if( zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "saa", &scope, &scope_len, &eq_cond, &range_cond ) == FAILURE ) {
    RETURN_FALSE;
  }
  
  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  if( NULL != hdex ) {
    try {

      //
      // Get the Equality Test Attributes...
      //
      arr_hash = Z_ARRVAL_P(eq_cond);

      eq_cond_attr = new hyperclient_attribute[zend_hash_num_elements( arr_hash )];      
      eq_attr_cnt = 0;
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;

        if( zend_hash_get_current_key_ex(arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING ) {
          buildAttrFromZval( *data, arr_key, &eq_cond_attr[eq_attr_cnt] );       
          eq_attr_cnt++;
        }
      }
      
      //
      // Get the Range Test Attributes...
      //
      arr_hash = Z_ARRVAL_P(range_cond);

      rng_cond_qry = new hyperclient_range_query[zend_hash_num_elements( arr_hash )];
      rng_attr_cnt = 0;
      
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer );
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

        char *arr_key;
        unsigned int arr_key_len;
        unsigned long index;

        if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING ) {
          buildRangeFromZval( *data, arr_key, &rng_cond_qry[rng_attr_cnt] );       
          rng_attr_cnt++;
        }
      }
      
      //
      // Build the query...
      //
      hyperclient_returncode op_status;
      hyperclient_attribute* attrs = NULL;
      size_t attrs_sz = 0;
      
      int64_t op_id = hdex->search( (const char*)scope,    /* the scope (table) */
                                    eq_cond_attr,           /* The equality attributes to search based on. */
                                    eq_attr_cnt,            /* The number of equality attributes */
                                    rng_cond_qry,           /* The range attributes to search based on */
                                    rng_attr_cnt,           /* The number of range attributes */
                                    &op_status,             /* where to put the status of the op */
                                    &attrs,                 /* Store the retrieved data here */ 
                                    &attrs_sz);             /* The number of attributes returned */

      if( op_id < 0 ) {
        freeAttrVals( eq_cond_attr, eq_attr_cnt );
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {
      
        array_init(return_value);        

        while( 1 == hyperdexLoop( hdex, op_id ) && HYPERCLIENT_SEARCHDONE != op_status ) {
          if( attrs_sz > 0 ) {
            zval *temp;
            ALLOC_INIT_ZVAL(temp);
          
            buildArrayFromAttrs( attrs, attrs_sz, temp );
          
            add_next_index_zval( return_value, temp );
          }
        }
        
        hyperclient_destroy_attrs( attrs, attrs_sz );        
        
        if( NULL != eq_cond_attr ) {
          freeAttrVals( eq_cond_attr, eq_attr_cnt );
          delete[] eq_cond_attr;
        }
        if( NULL != rng_cond_qry ) {
          delete[] rng_cond_qry;
        }
                       
        return;
      }
      
    } catch( int& e ) {     
      zend_throw_exception( hyperclient_ce_exception, "Bad Range Check", 1 TSRMLS_CC );
    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "Search failed", 1 TSRMLS_CC );
    }
  }

  if( NULL != eq_cond_attr ) {
    freeAttrVals( eq_cond_attr, eq_attr_cnt );
    delete[] eq_cond_attr;
  }
  
  if( NULL != rng_cond_qry ) {
    delete[] rng_cond_qry;
  }

  RETURN_FALSE;
}

/* }}} */

/* {{{ proto Array get(string space, string key)
   Returns the record identified by key from the specified space (table) */
PHP_METHOD( hyperclient, get ) {
  hyperclient*  hdex        = NULL;
  char*         scope       = NULL;
  int           scope_len   = -1;
  char*         key         = NULL;
  int           key_len     = -1;
  int           val_len     = -1;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ss", &scope, &scope_len, &key, &key_len ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC);
  hdex = obj->hdex;
  
  if( NULL != hdex ) {
    try {

      hyperclient_returncode op_status;
      hyperclient_attribute* attrs = NULL;
      size_t attrs_sz = 0;

      int64_t op_id = hdex->get( (const char*)scope,  /* we'll retrieve from the space*/
                                 (const char*)key,    /* we'll get the specified key */
                                 (size_t)key_len,      /* number of bytes in the key */ 
                                 &op_status,           /* We'll put the status here */
                                 &attrs,               /* Store the retrieved data here */
                                 &attrs_sz);           /* number of attributes in the retrieved record. */

      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {

        if( 1 == hyperdexLoop( hdex, op_id ) ) {

          buildArrayFromAttrs( attrs, attrs_sz, return_value );

          hyperclient_destroy_attrs( attrs, attrs_sz );
          
          return;
        }
      }
    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "get failed", 1 TSRMLS_CC );
    }
  }
  RETURN_FALSE;
}
/* }}} */

/* {{{ proto Mixed get(string space, string key, string attribute)
   Returns the named attribute from the record identified by key from the specified space (table) */
PHP_METHOD( hyperclient, get_attr ) {
  hyperclient*  hdex          = NULL;
  char*         scope         = NULL;
  int           scope_len     = -1;
  char*         key           = NULL;
  int           key_len       = -1;
  char*         attr_name     = NULL;
  int           attr_name_len = -1;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "sss", &scope, &scope_len, &key, &key_len, &attr_name, &attr_name_len ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  if( NULL != hdex ) {
    try {

      hyperclient_returncode op_status;
      hyperclient_attribute* attrs = NULL;
      size_t attrs_sz = 0;

      int64_t op_id = hdex->get( (const char*)scope,   /* we'll retrieve from the space*/
                                 (const char*)key,     /* we'll get the specified key */
                                 (size_t)key_len,       /* number of bytes in the key */ 
                                 &op_status,            /* We'll put the status here */
                                 &attrs,                /* Store the retrieved data here */
                                 &attrs_sz);            /* number of attributes in the retrieved record. */
                                 
      if( op_id < 0 ) {
        zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC );
      } else {

        if( 1 == hyperdexLoop( hdex, op_id ) ) {
          
          RETVAL_FALSE;
          
          if( attrs_sz > 0 ) {
            for( int i = 0; i < attrs_sz; ++i ) {

              if( strlen( attrs[i].attr ) == attr_name_len && strncmp( attrs[i].attr, attr_name, attrs[i].value_sz ) == 0 ) {
                buildZvalFromAttr( &attrs[i], return_value );
                break;
              }
              
            }
          }

          hyperclient_destroy_attrs( attrs, attrs_sz );
          return;
        }
      }
    } catch( ... ) {
      zend_throw_exception( hyperclient_ce_exception, "get failed", 1 TSRMLS_CC );
    }
  }

  RETURN_FALSE;
}
/* }}} */

/* {{{ proto Boolean del(string space, string key)
   Delete the record identified by key from the specified space */
PHP_METHOD( hyperclient, del ) {
  hyperclient*  hdex        = NULL;
  char*         scope       = NULL;
  int           scope_len   = -1;
  char*         key         = NULL;
  int           key_len     = -1;

  if( zend_parse_parameters( ZEND_NUM_ARGS() TSRMLS_CC, "ss", &scope, &scope_len, &key, &key_len ) == FAILURE ) {
    RETURN_FALSE;
  }

  hyperclient_object *obj = (hyperclient_object *)zend_object_store_get_object( getThis() TSRMLS_CC );
  hdex = obj->hdex;
  
  if( NULL != hdex ) {
    try {

      hyperclient_returncode op_status;
      hyperclient_attribute* attrs = NULL;
      size_t attrs_sz = 0;

      int64_t op_id = hdex->del( (const char*)scope,
                                 (const char*)key,
                                 (size_t)key_len,
                                 &op_status );

      if( op_id < 0 ) {
        zend_throw_exception(hyperclient_ce_exception, HyperDexErrorToMsg( op_status ), op_status TSRMLS_CC);
      } else {
        if( 1 == hyperdexLoop( hdex, op_id ) ) {
          RETURN_TRUE;
        }
      }
      
    } catch( ... ) {
      zend_throw_exception(hyperclient_ce_exception, "delete failed", 1 TSRMLS_CC);
    }
  }

  RETURN_FALSE;
}
/* }}} */


/**
 * Call the HyperDex receive loop so that we can get our data. 
 * Checks the return code from loop for errors, and throws the necessary
 * exceptions (based on loop status) if there is an error.
 */
int hyperdexLoop( hyperclient* hdex, int64_t op_id ) {
  hyperclient_returncode loop_status;
  
  int64_t loop_id = hdex->loop( -1,             /* wait forever */
                                &loop_status);  /*if something goes wrong, tell us*/
  
  if( loop_id < 0 ) {
    if( HYPERCLIENT_NONEPENDING != loop_status ) {
      zend_throw_exception( hyperclient_ce_exception, HyperDexErrorToMsg( loop_status ), loop_status TSRMLS_CC );
      return loop_status;
    }
      
    return -1;      
  }
  
  if( op_id != loop_id ) {
    // This is most certainly a bug for this simple code.
    zend_throw_exception( hyperclient_ce_exception, "Loop ID not equal Op ID", 2 TSRMLS_CC );
    return -1;
  }   

  return 1;
}


/**
 *  Converts a HyperDex returned byte array (little endian) Into a 64 bit long integer (regardless of platform)
 */
uint64_t byteArrayToUInt64( unsigned char *arr, size_t bytes ) {
  uint64_t num = 0ul;
  uint64_t tmp;

  size_t i;
  for( i = 0; i < bytes; i++ ) {
    tmp = 0ul;
    tmp = arr[i];
    num |= (tmp << ((i & 7) << 3));
  }
  return num;
}


/**
 *  Converts a 64 bit long integer into a HyperDex returned byte array (little endian) (regardless of platform)
 */
void uint64ToByteArray( uint64_t num, size_t bytes, unsigned char *arr ) {
  size_t        i;
  unsigned char ch;
  
  for( i = 0; i < bytes; i++  ) {
    ch = (num >> ((i & 7) << 3)) & 0xFF;
    arr[i] = ch;
  }
}


/**
 *  Converts a HyperDex returned byte array into a PHP array of C strings. ( Handles multibyte and long strings )
 */
void byteArrayToListString( unsigned char* arr, size_t bytes, zval* return_value ) {
  size_t cntr = 0;

  array_init( return_value );

  while( cntr < bytes ) {
    uint64_t byteCount = byteArrayToUInt64( arr + cntr, 4 );

    char* string = (char*)ecalloc( 1, byteCount+1 );
    memcpy( string, arr + cntr+4, byteCount );

    add_next_index_stringl( return_value, string, byteCount, 0 );

    cntr = cntr + 4 + byteCount;
  }
}


/**
 *  Converts a HyperDex returned byte array (little endian) Into a PHP array of 64 bit long integers (regardless of platform)
 */
void byteArrayToListInt64( unsigned char* arr, size_t bytes, zval* return_value ) {
  size_t cntr = 0;

  array_init(return_value);

  while( cntr < bytes ) {
    uint64_t val = byteArrayToUInt64( arr + cntr, 8 );
    add_next_index_long( return_value, val );
    cntr = cntr + 8;
  }
}


/**
 *  Converts a HyperDex returned byte array (IEEE 754) Into a PHP array of doubles
 */
void byteArrayToListFloat( unsigned char* arr, size_t bytes, zval* return_value ) {
  size_t cntr = 0;

  array_init(return_value);

  while( cntr < bytes ) {
    add_next_index_double( return_value, *( (double*)( arr + cntr ) ) );
    cntr = cntr + sizeof(double);
  }
}

/**
 *  Converts a HyperDex returned byte array into a PHP Map of C strings. ( Handles multibyte and long strings for value, not key.)
 *  (key = string, val = string)
 */
void byteArrayToMapStringString( unsigned char* arr, size_t bytes, zval* return_value ) {
  size_t cntr = 0;

  array_init( return_value );

  while( cntr < bytes ) {

    uint64_t byteCount = byteArrayToUInt64( arr + cntr, 4 );
    char* key = (char*)ecalloc( 1, byteCount+1 );
    memcpy( key, arr + cntr+4, byteCount );
	cntr = cntr + 4 + byteCount;

    byteCount = byteArrayToUInt64( arr + cntr, 4 );
    char* val = (char*)ecalloc( 1, byteCount+1 );
    memcpy( val, arr + cntr+4, byteCount );
    cntr = cntr + 4 + byteCount;

	add_assoc_stringl( return_value, key, val, byteCount, 0 );
  }
}


/**
 *  Converts a HyperDex returned byte array into a PHP Map of Long Integers. (key = string, val = integer)
 */
void byteArrayToMapStringInt64( unsigned char* arr, size_t bytes, zval* return_value ) {
  size_t cntr = 0;

  array_init( return_value );

  while( cntr < bytes ) {

    uint64_t byteCount = byteArrayToUInt64( arr + cntr, 4 );
    char* key = (char*)ecalloc( 1, byteCount+1 );
    memcpy( key, arr + cntr+4, byteCount );
	cntr = cntr + 4 + byteCount;

	uint64_t val = byteArrayToUInt64( arr + cntr, 8 );
    cntr = cntr + 8;

	add_assoc_long( return_value, key, val );

  }
}


/**
 *  Converts a HyperDex returned byte array into a PHP Map of Double floats. (key = string, val = double)
 */
void byteArrayToMapStringFloat( unsigned char* arr, size_t bytes, zval* return_value ) {
  size_t cntr = 0;

  array_init( return_value );

  while( cntr < bytes ) {

    uint64_t byteCount = byteArrayToUInt64( arr + cntr, 4 );
    
    char* key = (char*)ecalloc( 1, byteCount+1 );
    memcpy( key, arr + cntr+4, byteCount );
  	cntr = cntr + 4 + byteCount;

	  double val = *( (double*)( arr + cntr ));
    cntr = cntr + sizeof( double );

	  add_assoc_double( return_value, key, val );

  }
}


/**
 *  Converts a HyperDex returned byte array (little endian) Into a PHP map of strings with index as a number (regardless of platform)
 *  (key = integer, val = string)
 */
void byteArrayToMapInt64String( unsigned char* arr, size_t bytes, zval* return_value ) {
  size_t cntr = 0;

  array_init(return_value);

  while( cntr < bytes ) {
    
	uint64_t key_val = byteArrayToUInt64( arr + cntr, 8 );
    char* key = (char*)ecalloc( 1, 16 );
	sprintf(key, "%ld", key_val );
    cntr = cntr + 8;

    uint64_t byteCount = byteArrayToUInt64( arr + cntr, 4 );
    char* val = (char*)ecalloc( 1, byteCount+1 );
    memcpy( val, arr + cntr+4, byteCount );
	cntr = cntr + 4 + byteCount;

	add_assoc_stringl( return_value, key, val, byteCount, 0 );
  }
}


/**
 *  Converts a HyperDex returned byte array into a PHP Map of Long integers. (key = integer, val = integer)
 */
void byteArrayToMapInt64Int64( unsigned char* arr, size_t bytes, zval* return_value ) {
  size_t cntr = 0;

  array_init( return_value );

  while( cntr < bytes ) {

	uint64_t key_val = byteArrayToUInt64( arr + cntr, 8 );
    char* key = (char*)ecalloc( 1, 16 );
	sprintf(key, "%ld", key_val );
    cntr = cntr + 8;

	uint64_t val = byteArrayToUInt64( arr + cntr, 8 );
    cntr = cntr + 8;

	add_assoc_long( return_value, key, val );
  }
}

/**
 *  Converts a HyperDex returned byte array into a PHP Map of Double precision floats. (key = integer, val = double)
 */
void byteArrayToMapInt64Float( unsigned char* arr, size_t bytes, zval* return_value ) {
  size_t cntr = 0;

  array_init( return_value );

  while( cntr < bytes ) {

  	uint64_t key_val = byteArrayToUInt64( arr + cntr, 8 );
  	
    char* key = (char*)ecalloc( 1, 16 );
	  sprintf(key, "%ld", key_val );
    cntr = cntr + 8;

	  double val = *( (double*) arr + cntr );
    cntr = cntr + sizeof( double );

	  add_assoc_double( return_value, key, val );
  }
}


/**
 * Converts a PHP list of strings (multibyte safe) into a Byte Array suitable for HyperDex to store.
 */
void stringListToByteArray( zval* array, unsigned char** output_array, size_t *bytes, int sort ) {
  HashTable*   arr_hash = NULL;
  HashPosition pointer;
  zval**       data     = NULL;

  // Create a copy of the zval, so we don't overwrite the order of the original if we're sorting.
  zval temp_array = *array;
  zval_copy_ctor( &temp_array );
  arr_hash = Z_ARRVAL( temp_array );

  // Sort the list if we need to
  if( sort ) {
    zend_hash_sort ( arr_hash, zend_qsort, php_array_string_compare, 1 TSRMLS_CC );
  }


  int array_count = zend_hash_num_elements( arr_hash );

  // Start by getting the overall size of the memory block that we will need to allocate.
  size_t byteCount = 0;
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {
       
    zval temp = **data;
    zval_copy_ctor( &temp );
    convert_to_string( &temp );
    
    byteCount = byteCount + Z_STRLEN( temp ) + 4;
  }
  
  *bytes = byteCount;

  // Allocate it
  *output_array = (unsigned char*)calloc( 1, byteCount + 1 );

  // Now, fill it with the data that we want.2420
  size_t ptr = 0;
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {
       
    zval temp = **data;
    zval_copy_ctor( &temp );
    convert_to_string( &temp );

    // set the size of the current string element.
    uint64ToByteArray( Z_STRLEN( temp ) , 4, (*output_array) + ptr );

    // copy the string data
    memcpy( (*output_array) + 4 + ptr, Z_STRVAL( temp ), Z_STRLEN( temp ) );

    // move to the next string starting position
    ptr = ptr + 4 + Z_STRLEN( temp );
  }
}

/**
 * Converts a PHP list of integers (platform independent) into a Byte Array (little endian) suitable for HyperDex to store.
 */
void intListToByteArray( zval* array, unsigned char** output_array, size_t *bytes, int sort ) {
  HashTable*    arr_hash = NULL;
  HashPosition  pointer;
  zval**        data     = NULL;

  // Create a copy of the zval, so we don't overwrite the order of the original if we're sorting.
  zval temp_array = *array;
  zval_copy_ctor( &temp_array );
  arr_hash = Z_ARRVAL( temp_array );

  // Sort the list if we need to
  if( sort ) {
    zend_hash_sort ( arr_hash, zend_qsort, php_array_number_compare, 1 TSRMLS_CC );
  }

  int array_count = zend_hash_num_elements( arr_hash );

  // Start by getting the overall size of the memory block that we will need to allocate.
  size_t byteCount = 8 * array_count;

  *bytes = byteCount;

  // Allocate it
  *output_array = (unsigned char*)calloc( 1, byteCount + 1 );

  // Now, fill it with the data that we want.
  size_t ptr = 0;
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {
       
    if( Z_TYPE_PP( data ) == IS_LONG ) {

      // convert the long into an 8 byte array (little-endian) and store it
      uint64ToByteArray( Z_LVAL_PP( data ) , 8, *output_array + ptr );

      // move to the next position
      ptr = ptr + 8;
    }
  }
}


/**
 * Converts a PHP list of doubles (IEEE 754) into a Byte Array suitable for HyperDex to store.
 */
void doubleListToByteArray( zval* array, unsigned char** output_array, size_t *bytes, int sort ) {
  HashTable*    arr_hash = NULL;
  HashPosition  pointer;
  zval**        data     = NULL;

  // Create a copy of the zval, so we don't overwrite the order of the original if we're sorting.
  zval temp_array = *array;
  zval_copy_ctor( &temp_array );
  arr_hash = Z_ARRVAL( temp_array );

  // Sort the list if we need to
  if( sort ) {
    zend_hash_sort ( arr_hash, zend_qsort, php_array_number_compare, 1 TSRMLS_CC );
  }

  int array_count = zend_hash_num_elements( arr_hash );

  // Start by getting the overall size of the memory block that we will need to allocate.
  size_t byteCount = sizeof(double) * array_count;

  *bytes = byteCount;

  // Allocate it
  *output_array = (unsigned char*)calloc( 1, byteCount + 1 );

  // Now, fill it with the data that we want.
  size_t ptr = 0;
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS;
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {
       
    if( Z_TYPE_PP( data ) == IS_DOUBLE ) {

      // convert the double into a byte array and store it
      memcpy( *output_array + ptr, (double*)&Z_DVAL_PP( data ), sizeof(double) );
    
      // move to the next position
      ptr = ptr + sizeof(double);
    }
  }
}


/**
 * Converts a PHP hash of strings into a Byte Array suitable for HyperDex to store.
 */
void stringStringHashToByteArray( zval* array, unsigned char** output_array, size_t *bytes ) {
  HashTable*     arr_hash = NULL;
  HashPosition   pointer;
  char*            arr_key;
  unsigned int    arr_key_len;
  unsigned long   index;

  zval**        data     = NULL;

  arr_hash = Z_ARRVAL_P( array );
  int array_count = zend_hash_num_elements( arr_hash );

  // Start by getting the overall size of the memory block that we will need to allocate.
  size_t byteCount = 0;
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

    if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
      byteCount = byteCount + arr_key_len + 4;

	  zval temp = **data;
      zval_copy_ctor( &temp );
      convert_to_string( &temp );
    
      byteCount = byteCount + Z_STRLEN( temp ) + 4;
	}
  }
  
  *bytes = byteCount;

  // Allocate it
  *output_array = (unsigned char*)calloc( 1, byteCount + 1 );

  // Now, fill it with the data that we want.
  size_t ptr = 0;
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

    if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
   
      // set the size of the current string key.
      uint64ToByteArray( arr_key_len , 4, (*output_array) + ptr );

      // copy the string data
      memcpy( (*output_array) + 4 + ptr, arr_key, arr_key_len );

      // move to the next string starting position
      ptr = ptr + 4 + arr_key_len;

      //
	  // Now, make sure the data element for this entry is a string
	  //
      zval temp = **data;
      zval_copy_ctor( &temp );
      convert_to_string( &temp );

      // set the size of the current string element.
      uint64ToByteArray( Z_STRLEN( temp ) , 4, (*output_array) + ptr );

      // copy the string data
      memcpy( (*output_array) + 4 + ptr, Z_STRVAL( temp ), Z_STRLEN( temp ) );

      // move to the next string starting position
      ptr = ptr + 4 + Z_STRLEN( temp );
    }
  }
}

/**
 * Converts a PHP hash of Integers ( key=string, value=integer ) into a Byte Array suitable for HyperDex to store.
 */
void stringInt64HashToByteArray( zval* array, unsigned char** output_array, size_t *bytes ) {
  HashTable*       arr_hash = NULL;
  HashPosition     pointer;
  char*            arr_key;
  unsigned int    arr_key_len;
  unsigned long   index;
  zval**           data     = NULL;

  arr_hash = Z_ARRVAL_P( array );
  int array_count = zend_hash_num_elements( arr_hash );

  // Start by getting the overall size of the memory block that we will need to allocate.
  size_t byteCount = 0;
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {
    if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
      byteCount = byteCount + arr_key_len + 4 + 8;
	  }
  }
  
  *bytes = byteCount;

  // Allocate it
  *output_array = (unsigned char*)calloc( 1, byteCount + 1 );

  // Now, fill it with the data that we want.
  size_t ptr = 0;
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

    if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
   
      // set the size of the current string key.
      uint64ToByteArray( arr_key_len , 4, (*output_array) + ptr );

      // copy the string data
      memcpy( (*output_array) + 4 + ptr, arr_key, arr_key_len );

      // move to the next string starting position
      ptr = ptr + 4 + arr_key_len;

      // The data is an int64, so convert it
	  uint64ToByteArray( Z_LVAL_PP(data) , 8, *output_array + ptr );

      // move to the next position
      ptr = ptr + 8;
    }
  }
}


/**
 * Converts a PHP hash of Doubles ( key=string, value=double ) into a Byte Array suitable for HyperDex to store.
 */
void stringDoubleHashToByteArray( zval* array, unsigned char** output_array, size_t *bytes ) {
  HashTable*       arr_hash = NULL;
  HashPosition     pointer;
  char*            arr_key;
  unsigned int    arr_key_len;
  unsigned long   index;
  zval**           data     = NULL;

  arr_hash = Z_ARRVAL_P( array );
  int array_count = zend_hash_num_elements( arr_hash );

  // Start by getting the overall size of the memory block that we will need to allocate.
  size_t byteCount = 0;
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {
    if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
      byteCount = byteCount + arr_key_len + 4 + sizeof( double);
	  }
  }
  
  *bytes = byteCount;

  // Allocate it
  *output_array = (unsigned char*)calloc( 1, byteCount + 1 );

  // Now, fill it with the data that we want.
  size_t ptr = 0;
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

    if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING) {
   
      // set the size of the current string key.
      uint64ToByteArray( arr_key_len , 4, (*output_array) + ptr );

      // copy the string data
      memcpy( (*output_array) + 4 + ptr, arr_key, arr_key_len );

      // move to the next string starting position
      ptr = ptr + 4 + arr_key_len;

      // The data is a double, so convert it
      memcpy( *output_array + ptr, (double*)&Z_DVAL_PP( data ), sizeof( double ) );

      // move to the next position
      ptr = ptr + sizeof( double );
    }
  }
}


/**
 * Converts a PHP hash of Integers ( key=integer, value=integer ) into a Byte Array suitable for HyperDex to store.
 */
void int64Int64HashToByteArray( zval* array, unsigned char** output_array, size_t *bytes ) {
  HashTable*       arr_hash = NULL;
  HashPosition     pointer;
  char*            arr_key;
  unsigned int    arr_key_len;
  unsigned long   index;
  zval**           data     = NULL;

  arr_hash = Z_ARRVAL_P( array );
  int array_count = zend_hash_num_elements( arr_hash );

  // Start by getting the overall size of the memory block that we will need to allocate.
  size_t byteCount = 0;
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {
    if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_LONG) {
      byteCount = byteCount + 16;
	}
  }
  
  *bytes = byteCount;

  // Allocate it
  *output_array = (unsigned char*)calloc( 1, byteCount + 1 );

  // Now, fill it with the data that we want.
  size_t ptr = 0;
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

    if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_LONG) {
   
      // set the size of the current string key.
      uint64ToByteArray( index , 8, (*output_array) + ptr );

      // move to the next string starting position
      ptr = ptr + 8;

      // The data is an int64, so convert it
	  uint64ToByteArray( Z_LVAL_PP(data) , 8, *output_array + ptr );

      // move to the next position
      ptr = ptr + 8;
    }
  }
}


/**
 * Converts a PHP hash of Stings ( key=integer, value=string ) into a Byte Array suitable for HyperDex to store.
 */
void int64StringHashToByteArray( zval* array, unsigned char** output_array, size_t *bytes ) {
  HashTable*       arr_hash = NULL;
  HashPosition     pointer;
  char*            arr_key;
  unsigned int    arr_key_len;
  unsigned long   index;
  zval**           data     = NULL;

  arr_hash = Z_ARRVAL_P( array );
  int array_count = zend_hash_num_elements( arr_hash );

  // Start by getting the overall size of the memory block that we will need to allocate.
  size_t byteCount = 0;
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {
    if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_LONG) {
      zval temp = **data;
      zval_copy_ctor( &temp );
      convert_to_string( &temp );
    
      byteCount = byteCount + Z_STRLEN( temp ) + 12;       
	}
  }
  
  *bytes = byteCount;

  // Allocate it
  *output_array = (unsigned char*)calloc( 1, byteCount + 1 );

  // Now, fill it with the data that we want.
  size_t ptr = 0;
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {

    if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_LONG) {
   
      // set the size of the current string key.
      uint64ToByteArray( index , 8, (*output_array) + ptr );

      // move to the next string starting position
      ptr = ptr + 8;

      // The data is a string, so convert it
      zval temp = **data;
      zval_copy_ctor( &temp );
      convert_to_string( &temp );
      
      // set the size of the current string element.
      uint64ToByteArray( Z_STRLEN( temp ) , 4, (*output_array) + ptr );

      // copy the string data
      memcpy( (*output_array) + 4 + ptr, Z_STRVAL( temp ), Z_STRLEN( temp ) );

      // move to the next string starting position
      ptr = ptr + 4 + Z_STRLEN( temp );
    
    }
  }
}


/**
 *  Iterates through a list, and determines if the array is composed entirely of integers or not.
 */
int isArrayAllLong( zval* array ) {
  HashTable*   arr_hash     = NULL;
  HashPosition pointer;
  char*            arr_key;
  unsigned int    arr_key_len;
  unsigned long   index;
  zval**       data         = NULL;
  int          return_value = 1;

  arr_hash = Z_ARRVAL_P( array );
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {
       
    if( Z_TYPE_PP(data) != IS_LONG ) {
      return_value = 0;
      break;
    }
  }

  return return_value;
}

/**
 *  Iterates through a list, and determines if the array is composed entirely of doubles or not.
 */
int isArrayAllDouble( zval* array ) {
  HashTable*   arr_hash     = NULL;
  HashPosition pointer;
  char*            arr_key;
  unsigned int    arr_key_len;
  unsigned long   index;
  zval**       data         = NULL;
  int          return_value = 1;

  arr_hash = Z_ARRVAL_P( array );
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {
       
    if( Z_TYPE_PP(data) != IS_DOUBLE ) {
      return_value = 0;
      break;
    }
  }

  return return_value;
}


/**
 *  Iterates through a list, and determines if the array is a hash table (string keys intead of indexes )
 */
int isArrayHashTable( zval* array ) {
  HashTable*       arr_hash     = NULL;
  HashPosition     pointer;
  char*            arr_key;
  unsigned int    arr_key_len;
  unsigned long   index;  
  zval**           data         = NULL;
  int              return_value = 0;

  arr_hash = Z_ARRVAL_P( array );
  for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
       zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
       zend_hash_move_forward_ex( arr_hash, &pointer ) ) {
       
    if( zend_hash_get_current_key_ex( arr_hash, &arr_key, &arr_key_len, &index, 0, &pointer ) == HASH_KEY_IS_STRING ) {
      return_value = 1;
      break;
    }
  }

  return return_value;
}


/**
 * Build a HyperDex ATTR structure from a PHP ZVAL structure.
 */
void buildAttrFromZval( zval* data, char* arr_key, hyperclient_attribute* attr, enum hyperdatatype expected_type ) {

  zval temp = *data;
  zval_copy_ctor(&temp);

  attr->attr = arr_key;

  if( Z_TYPE( temp ) == IS_STRING ) {            
  
    //
    // Convert a string
    //
    attr->value = Z_STRVAL(temp);
    attr->value_sz = Z_STRLEN(temp);
    attr->datatype = HYPERDATATYPE_STRING;
    
  } else if( Z_TYPE( temp ) == IS_LONG ) {
  
    //
    // Convert a long (integer)
    //
    unsigned char* val = (unsigned char*)calloc( 1, 9 );    
    uint64ToByteArray( Z_LVAL( temp ), 8, val );    
    
    attr->value = (const char*)val;
    attr->value_sz = 8;
    attr->datatype = HYPERDATATYPE_INT64;
    
  } else if( Z_TYPE( temp ) == IS_DOUBLE ) {    

    //
    // Convert a Double (floating point number)
    //
    char* val = (char*)calloc( 1, sizeof(double) );
    memcpy( val, (double*)&Z_DVAL( temp ), sizeof(double) );
    
    attr->value = (const char*)val;
    attr->value_sz = sizeof(double);
    attr->datatype = HYPERDATATYPE_FLOAT;
    
  } else if( Z_TYPE( temp ) == IS_ARRAY ) {
  
  //
  // Convert an array into a list (it will either be all integers, all doubles, or all strings)
  //    
  unsigned char* val = NULL;
  size_t num_bytes = 0;
      	
	if( 1 == isArrayHashTable( &temp ) ) {
	  //
	  // If the keys for the array are all alpha-numeric, we have a PHP Hash..
	  // We will make that a string/<x> Map for Hyperdex.
	  //
    
	  
	  //
	  // Check the type in the value of each element. 
	  // If all are integers, make it a hash of ints. 
	  // Else, if all are doubles, make it a hash of doubles.
	  // Else, by default, convert them to string, and make it a hash of strings.
	  //
	  if( 1 == isArrayAllLong( &temp ) ) {
      // HashStringInt64
      stringInt64HashToByteArray( &temp, &val, &num_bytes );
      attr->datatype = HYPERDATATYPE_MAP_STRING_INT64;
	  } else if( 1 == isArrayAllDouble( &temp ) ) {
	    // HashStringFloat
	    stringDoubleHashToByteArray( &temp, &val, &num_bytes );
      attr->datatype = HYPERDATATYPE_MAP_STRING_FLOAT;
	  } else {
      // HashStringString
      stringStringHashToByteArray( &temp, &val, &num_bytes );
      attr->datatype = HYPERDATATYPE_MAP_STRING_STRING;
	  }
	  
/*	} else if( expected_type > HYPERDATATYPE_MAP_INT64_KEYONLY && 
	            expected_type < HYPERDATATYPE_MAP_FLOAT_KEYONLY ) {
	  //
	  // We have a PHP list, but HyperDex is expecting a Map with numeric keys.
	  //	
	  
	  // are the value all integers?
	  if( 1 == isArrayAllLong( &temp ) ) {
           // HashInt64Int64
           int64Int64HashToByteArray( &temp, &val, &num_bytes );
	  } else {
           // HashInt64String
           int64StringHashToByteArray( &temp, &val, &num_bytes );
	  }
*/	  
	}  else {
	  //
	  // Else, we will have a PHP List and HyperDex wants a list or set, which is OK...
	  //
	  
      if( 1 == isArrayAllLong( &temp ) ) {
    
        //
        // Long Integer array or set
        //              
        
        // A set needs to be sorted...  
        int sort = 0;
        if( expected_type == HYPERDATATYPE_SET_INT64 ) {
          sort = 1;
        }

        intListToByteArray( &temp, &val, &num_bytes, sort );
        
        if( expected_type == HYPERDATATYPE_SET_INT64 ) {
          attr->datatype = HYPERDATATYPE_SET_INT64;
        } else {        
          attr->datatype = HYPERDATATYPE_LIST_INT64;
        }
      
      } else if( 1 == isArrayAllDouble( &temp ) ) {
        //
        // Double precision float array or set
        //                
        
        // A set needs to be sorted...
        int sort = 0;
        if( expected_type == HYPERDATATYPE_SET_FLOAT ) {
          sort = 1;
        }

        doubleListToByteArray( &temp, &val, &num_bytes, sort );
         
        if( expected_type == HYPERDATATYPE_SET_FLOAT ) {
          attr->datatype = HYPERDATATYPE_SET_FLOAT;
        } else {        
          attr->datatype = HYPERDATATYPE_LIST_FLOAT;
        }      
      } else {
       
        //
        // String array or set (also the default if the incoming array is mixed value type)
        //        
        
        // A set needs to be sorted...
        int sort = 0;
        if( expected_type == HYPERDATATYPE_SET_STRING ) {
          sort = 1;
        }

        stringListToByteArray( &temp, &val, &num_bytes, sort );
        
        if( expected_type == HYPERDATATYPE_SET_STRING ) {
          attr->datatype = HYPERDATATYPE_SET_STRING;
        } else {        
          attr->datatype = HYPERDATATYPE_LIST_STRING;
        }

      }
	}
    // 
    // Now assign the array character buffer to the value.
    //
    attr->value = (const char*)val;
    attr->value_sz = num_bytes;
  }    
  
}


/**
 * Take a list of attributes, and build an Hash Array ZVAL out of it.
 */
void buildArrayFromAttrs( hyperclient_attribute* attrs, size_t attrs_sz, zval* data ) {
  
  array_init(data);          
  
  if( attrs_sz > 0 ) {
    for( int i = 0; i < attrs_sz; ++i ) {

      char* attr_name = estrdup( attrs[i].attr );

      zval *mysubdata;
      ALLOC_INIT_ZVAL(mysubdata);
      
      buildZvalFromAttr( &attrs[i], mysubdata );      
      add_assoc_zval( data, attr_name, mysubdata );    
    }
  } else {
    ZVAL_BOOL( data, 0 );
  }
}
  
   
/**
 *  Build a ZVAL frmo a specific HyperDex attribute.
 */
void buildZvalFromAttr( hyperclient_attribute* attr, zval* data ) {

  if( attr->datatype == HYPERDATATYPE_STRING ) {
    char* r_val = estrndup((char *)attr->value, attr->value_sz);
    ZVAL_STRINGL( data, r_val, attr->value_sz, 0 );
  } else if( attr->datatype == HYPERDATATYPE_INT64 ) {  
    ZVAL_LONG( data, byteArrayToUInt64( (unsigned char*)attr->value, attr->value_sz) );
  } else if( attr->datatype == HYPERDATATYPE_FLOAT ) {  
    ZVAL_DOUBLE( data, *( (double*) attr->value ) );
  } else if( attr->datatype == HYPERDATATYPE_LIST_STRING ||
	            attr->datatype == HYPERDATATYPE_SET_STRING ) {
    byteArrayToListString( (unsigned char*)attr->value, attr->value_sz, data );
  } else if( attr->datatype == HYPERDATATYPE_LIST_INT64 || 
	            attr->datatype == HYPERDATATYPE_SET_INT64 ) {
    byteArrayToListInt64( (unsigned char*)attr->value, attr->value_sz, data );
  } else if( attr->datatype == HYPERDATATYPE_LIST_FLOAT || 
	            attr->datatype == HYPERDATATYPE_SET_FLOAT ) {
    byteArrayToListFloat( (unsigned char*)attr->value, attr->value_sz, data );    
  } else if( attr->datatype == HYPERDATATYPE_MAP_STRING_STRING ) {
    byteArrayToMapStringString( (unsigned char*)attr->value, attr->value_sz, data );
  } else if( attr->datatype == HYPERDATATYPE_MAP_STRING_INT64 ) {
    byteArrayToMapStringInt64( (unsigned char*)attr->value, attr->value_sz, data );
  } else if( attr->datatype == HYPERDATATYPE_MAP_STRING_FLOAT ) {
    byteArrayToMapStringFloat( (unsigned char*)attr->value, attr->value_sz, data );
  } else if( attr->datatype == HYPERDATATYPE_MAP_INT64_STRING ) {
    byteArrayToMapInt64String( (unsigned char*)attr->value, attr->value_sz, data );
  } else if( attr->datatype == HYPERDATATYPE_MAP_INT64_INT64 ) {
    byteArrayToMapInt64Int64( (unsigned char*)attr->value, attr->value_sz, data );    
  } else if( attr->datatype == HYPERDATATYPE_MAP_INT64_FLOAT ) {
    byteArrayToMapInt64Float( (unsigned char*)attr->value, attr->value_sz, data );
    
  } else {
    zend_throw_exception(hyperclient_ce_exception, "Unknown data type", attr->datatype TSRMLS_CC);
  }
}


/**
 * Build a HyperDex Range Query structure from a PHP ZVAL structure.
 */
void buildRangeFromZval( zval* input, char* arr_key, hyperclient_range_query* rng_q ) {

  zval temp = *input;
  zval_copy_ctor(&temp);

  if( Z_TYPE( temp ) == IS_ARRAY ) {     

    if( 1 == isArrayAllLong( &temp ) ) {

      HashTable *arr_hash = Z_ARRVAL(temp);
      HashPosition pointer;
      zval **data;
      
      uint64_t array_vals[2] = {0, 0};
      int cntr = 0;
      for( zend_hash_internal_pointer_reset_ex( arr_hash, &pointer ); 
           zend_hash_get_current_data_ex( arr_hash, (void**) &data, &pointer ) == SUCCESS; 
           zend_hash_move_forward_ex( arr_hash, &pointer ) ) {
           
        if( Z_TYPE_PP(data) == IS_LONG ) {
           array_vals[cntr++] = Z_LVAL_PP( data );
           if( cntr == 2 ) break;
        }
      }
      
      if( cntr != 2 ) {
        // Error - The input array exactly 2 integers.
        zend_throw_exception(hyperclient_ce_exception, "Invalid range values", HYPERCLIENT_WRONGTYPE TSRMLS_CC);
        throw -1;
      } else {

        rng_q->attr = arr_key;      
        rng_q->lower = array_vals[0];
        rng_q->upper = array_vals[1];
      }
      
    } else {    
      // Error - The input array should contain all integers.
      zend_throw_exception(hyperclient_ce_exception, "Invalid range values", HYPERCLIENT_WRONGTYPE TSRMLS_CC);
      throw -1;
    }
        
  } else {
    // The input *MUST* be an array.
    zend_throw_exception(hyperclient_ce_exception, "Invalid range type", HYPERCLIENT_WRONGTYPE TSRMLS_CC);
    throw -1;
  }
    
}

/**
 *  Clean up memory allocated by the system for storing HyperDex values.
 */
void freeAttrVals( hyperclient_attribute *attr, int len ) {
  for( int i = 0; i < len; ++i ) {
    switch( attr[i].datatype ) {
      case HYPERDATATYPE_INT64:
      case HYPERDATATYPE_FLOAT:
      case HYPERDATATYPE_LIST_STRING:
      case HYPERDATATYPE_LIST_INT64:
      case HYPERDATATYPE_SET_STRING:
      case HYPERDATATYPE_SET_INT64:
      case HYPERDATATYPE_MAP_STRING_INT64:
      case HYPERDATATYPE_MAP_STRING_STRING:
      case HYPERDATATYPE_MAP_INT64_STRING:
      case HYPERDATATYPE_MAP_INT64_INT64:
        if( attr[i].value != NULL ) {
          free( (void*)attr[i].value );
          attr[i].value = NULL;
        }
        break;
    }
  }
}
 
/*
 * Borrowed from PHP iteslf
 */
static int php_array_string_compare(const void *a, const void *b TSRMLS_DC) /* {{{ */
{    
	Bucket *f;
	Bucket *s;
	zval result;
	zval *first;
	zval *second;

	f = *((Bucket **) a);
	s = *((Bucket **) b);

	first = *((zval **) f->pData);
	second = *((zval **) s->pData);

	if (string_compare_function(&result, first, second TSRMLS_CC) == FAILURE) {
		return 0;
	}

	if (Z_TYPE(result) == IS_DOUBLE) {
		if (Z_DVAL(result) < 0) {
			return -1;
		} else if (Z_DVAL(result) > 0) {
			return 1;
		} else {
			return 0;
		}
	}

	convert_to_long(&result);

	if (Z_LVAL(result) < 0) {
		return -1;
	} else if (Z_LVAL(result) > 0) {
		return 1;
	}

	return 0;
}
/* }}} */

static int php_array_number_compare(const void *a, const void *b TSRMLS_DC) /* {{{ */
{
	Bucket *f;
	Bucket *s;
	zval result;
	zval *first;
	zval *second;

	f = *((Bucket **) a);
	s = *((Bucket **) b);

	first = *((zval **) f->pData);
	second = *((zval **) s->pData);

	if (numeric_compare_function(&result, first, second TSRMLS_CC) == FAILURE) {
		return 0;
	}

	if (Z_TYPE(result) == IS_DOUBLE) {
		if (Z_DVAL(result) < 0) {
			return -1;
		} else if (Z_DVAL(result) > 0) {
			return 1;
		} else {
			return 0;
		}
	}

	convert_to_long(&result);

	if (Z_LVAL(result) < 0) {
		return -1;
	} else if (Z_LVAL(result) > 0) {
		return 1;
	}

	return 0;
}
/* }}} */


/**
 * Translate HyperDex error / status codes into strings that can be used in exceptions.
 */
char* HyperDexErrorToMsg( hyperclient_returncode code ) {

  switch( code ) {
    /* General Statuses */
    case HYPERCLIENT_SUCCESS: return("Success");
    case HYPERCLIENT_NOTFOUND: return("Not Found");
    case HYPERCLIENT_SEARCHDONE: return("Search Done");
    case HYPERCLIENT_CMPFAIL: return("Compare Failed");
    case HYPERCLIENT_READONLY: return("Read Only");

    /* Error conditions */
    case HYPERCLIENT_UNKNOWNSPACE: return("Unknown Space");
    case HYPERCLIENT_COORDFAIL: return("Coordinator Failure");
    case HYPERCLIENT_SERVERERROR: return("Server Error");
    case HYPERCLIENT_POLLFAILED: return("Poll Failed");
    case HYPERCLIENT_OVERFLOW: return("Overflow");
    case HYPERCLIENT_RECONFIGURE: return("Reconfigure");
    case HYPERCLIENT_TIMEOUT: return("Timeout");
    case HYPERCLIENT_UNKNOWNATTR: return("Unknown Attribute");
    case HYPERCLIENT_DUPEATTR: return("Duplicate Attribute");
    case HYPERCLIENT_NONEPENDING: return("None Pending");
    case HYPERCLIENT_DONTUSEKEY: return("Dont Use Key");
    case HYPERCLIENT_WRONGTYPE: return("Wrong Attribute Type");
    case HYPERCLIENT_NOMEM: return("Out of Memory");
  }
}

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
