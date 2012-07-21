/*
  +----------------------------------------------------------------------+
  | HyperDex client plugin                                               |
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

#ifndef PHP_HYPERDEX_H
#define PHP_HYPERDEX_H

extern zend_module_entry hyperdex_module_entry;
#define phpext_hyperdex_ptr &hyperdex_module_entry

#ifdef PHP_WIN32
#	define PHP_HYPERDEX_API __declspec(dllexport)
#elif defined(__GNUC__) && __GNUC__ >= 4
#	define PHP_HYPERDEX_API __attribute__ ((visibility("default")))
#else
#	define PHP_HYPERDEX_API
#endif

#ifdef ZTS
#include "TSRM.h"
#endif

PHP_MINIT_FUNCTION(hyperdex);
PHP_MSHUTDOWN_FUNCTION(hyperdex);
PHP_RINIT_FUNCTION(hyperdex);
PHP_RSHUTDOWN_FUNCTION(hyperdex);
PHP_MINFO_FUNCTION(hyperdex);

PHP_METHOD(hyperclient, __construct);
PHP_METHOD(hyperclient, __destruct);
PHP_METHOD(hyperclient, connect);
PHP_METHOD(hyperclient, disconnect);

PHP_METHOD(hyperclient, put);
PHP_METHOD(hyperclient, put_attr);
PHP_METHOD(hyperclient, condput);

PHP_METHOD(hyperclient, lpush);
PHP_METHOD(hyperclient, rpush);

PHP_METHOD(hyperclient, set_add);
PHP_METHOD(hyperclient, set_remove);
PHP_METHOD(hyperclient, set_union);
PHP_METHOD(hyperclient, set_intersect);

PHP_METHOD(hyperclient, add);
PHP_METHOD(hyperclient, sub);
PHP_METHOD(hyperclient, mul);
PHP_METHOD(hyperclient, div);
PHP_METHOD(hyperclient, mod);
PHP_METHOD(hyperclient, and);
PHP_METHOD(hyperclient, or);
PHP_METHOD(hyperclient, xor);
    
PHP_METHOD(hyperclient, search);
PHP_METHOD(hyperclient, get);
PHP_METHOD(hyperclient, get_attr);

PHP_METHOD(hyperclient, del);

/* 
  	Declare any global variables you may need between the BEGIN
	and END macros here:     
*/
ZEND_BEGIN_MODULE_GLOBALS(hyperdex)  
ZEND_END_MODULE_GLOBALS(hyperdex)

/* In every utility function you add that needs to use variables 
   in php_hyperdex_globals, call TSRMLS_FETCH(); after declaring other 
   variables used by that function, or better yet, pass in TSRMLS_CC
   after the last function argument and declare your utility function
   with TSRMLS_DC after the last declared argument.  Always refer to
   the globals in your function as HYPERDEX_G(variable).  You are 
   encouraged to rename these macros something shorter, see
   examples in any other php module directory.
*/

#ifdef ZTS
#define HYPERDEX_G(v) TSRMG(hyperdex_globals_id, zend_hyperdex_globals *, v)
#else
#define HYPERDEX_G(v) (hyperdex_globals.v)
#endif

#endif	/* PHP_HYPERDEX_H */


/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
