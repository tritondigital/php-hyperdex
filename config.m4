dnl $Id$
dnl config.m4 for extension hyperdex

dnl Comments in this file start with the string 'dnl'.
dnl Remove where necessary. This file will not work
dnl without editing.

dnl If your extension references something external, use with:

dnl PHP_ARG_WITH(hyperdex, for hyperdex support,
dnl Make sure that the comment is aligned:
dnl [  --with-hyperdex             Include hyperdex support])

dnl Otherwise use enable:

PHP_ARG_ENABLE(hyperdex, whether to enable hyperdex support,
Make sure that the comment is aligned:
[  --enable-hyperdex           Enable hyperdex support])

if test "$PHP_HYPERDEX" != "no"; then
  dnl Write more examples of tests here...

  PHP_REQUIRE_CXX()
  PHP_SUBST(HYPERDEX_SHARED_LIBADD)
  PHP_ADD_LIBRARY(stdc++, 1, HYPERDEX_SHARED_LIBADD)
  PHP_ADD_LIBRARY(hyperclient, 1, HYPERDEX_SHARED_LIBADD)

  dnl # --with-hyperdex -> check with-path
  dnl SEARCH_PATH="/usr/local /usr"     # you might want to change this
  dnl SEARCH_FOR="/include/hyperdex.h"  # you most likely want to change this
  dnl if test -r $PHP_HYPERDEX/$SEARCH_FOR; then # path given as parameter
  dnl   HYPERDEX_DIR=$PHP_HYPERDEX
  dnl else # search default path list
  dnl   AC_MSG_CHECKING([for hyperdex files in default path])
  dnl   for i in $SEARCH_PATH ; do
  dnl     if test -r $i/$SEARCH_FOR; then
  dnl       HYPERDEX_DIR=$i
  dnl       AC_MSG_RESULT(found in $i)
  dnl     fi
  dnl   done
  dnl fi
  dnl
  dnl if test -z "$HYPERDEX_DIR"; then
  dnl   AC_MSG_RESULT([not found])
  dnl   AC_MSG_ERROR([Please reinstall the hyperdex distribution])
  dnl fi

  dnl # --with-hyperdex -> add include path
  dnl PHP_ADD_INCLUDE($HYPERDEX_DIR/include)

  dnl # --with-hyperdex -> check for lib and symbol presence
  dnl LIBNAME=hyperdex # you may want to change this
  dnl LIBSYMBOL=hyperdex # you most likely want to change this 

  dnl PHP_CHECK_LIBRARY($LIBNAME,$LIBSYMBOL,
  dnl [
  dnl   PHP_ADD_LIBRARY_WITH_PATH($LIBNAME, $HYPERDEX_DIR/lib, HYPERDEX_SHARED_LIBADD)
  dnl   AC_DEFINE(HAVE_HYPERDEXLIB,1,[ ])
  dnl ],[
  dnl   AC_MSG_ERROR([wrong hyperdex lib version or lib not found])
  dnl ],[
  dnl   -L$HYPERDEX_DIR/lib -lm
  dnl ])
  dnl
  dnl PHP_SUBST(HYPERDEX_SHARED_LIBADD)

  PHP_NEW_EXTENSION(hyperdex, hyperdex.cc, $ext_shared)
fi
