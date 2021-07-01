/**
 * DataSight Spark HiveODBC example
 * Compile this with Visual Studio 2012
 */

#ifdef _WIN32
#include <windows.h>
#endif
#include <sqlext.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

/**
 * Check ODBC operation return status
 */
int CHECK_STATUS(SQLRETURN rc, SQLCHAR *location, SQLHANDLE hnd, SQLSMALLINT hType) {
  if (rc == SQL_SUCCESS) {
    printf("  ** %s: ok\n", location);
  } else {
    CHAR *status = (rc == SQL_SUCCESS_WITH_INFO) ? "OK" : "failed" ;
    SQLRETURN _rc;
    SQLCHAR state[5 + 1];
    SQLINTEGER ecode;
    SQLSMALLINT msgLength;
    int recNumber = 0;

    printf(" ** %s: %s. rc(%d)\n", location, status, rc);

    do {
      _rc = SQLGetDiagRec(hType,
                          hnd,
                          ++recNumber, // RecNumber,
                          state,       // SQLState,
                          &ecode,      // NativeErrorPtr,
                          NULL,        // MessageText,
                          0,           // BufferLength,
                          &msgLength);
      if (_rc == SQL_NO_DATA) {
        // do nothing
      } else if (_rc != SQL_SUCCESS) {
        printf(" ** Cannot retrieve error message, reason: %d\n", _rc);
      } else {
        if (recNumber == 1) {
          printf(" ** SQL State: %s\n", state);
        }
        if (rc != SQL_SUCCESS_WITH_INFO) {
          printf(" ** error code: %d\n", ecode);
        }
        if (msgLength > 0) {
          SQLCHAR * msg = (SQLCHAR *)malloc((msgLength + 2) * sizeof(SQLCHAR));
          _rc = SQLGetDiagRec(hType,
                              hnd,
                              recNumber,   // RecNumber,
                              state,       // SQLState,
                              &ecode,      // NativeErrorPtr,
                              msg,         // MessageText,
                              msgLength+2, // BufferLength,
                              &msgLength);
          printf(" ** msg %d: %s\n", recNumber, msg);
          free(msg);
        }
      }
    } while (_rc == SQL_SUCCESS);
  }

  return (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO);
}


int CHECK_STATUS_DBC(SQLRETURN rc, SQLCHAR *location, SQLHANDLE hnd) {
  return CHECK_STATUS(rc, location, hnd, SQL_HANDLE_DBC);
}


int CHECK_STATUS_ENV(SQLRETURN rc, SQLCHAR *location, SQLHANDLE hnd) {
  return CHECK_STATUS(rc, location, hnd, SQL_HANDLE_ENV);
}

/**
 * Call SQLExecDirect and SQLFetch to run given SQL and fetch result from server
 */
void viewFetchresults(SQLHDBC hdbc, SQLCHAR * SQL) {
  SQLRETURN sr;
  SQLHSTMT hstmt;
  // Column Date Variables
  struct {
    SQLCHAR tab_name[51];
    SQLINTEGER nameLength;
  } row;
  char message[4096];

  strcpy (message, "Fetch Results:\n"); //Initialize

  // Allocate Statement Handle
  sr = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) {
    printf("Error in allocating statement in OnViewfetchresults");
  }
  // Execute SQL statement
  sr = SQLExecDirect(hstmt, SQL, SQL_NTS);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) {
    printf("Error executing statement in OnViewfetchresults");
  }
  // Bind each column
  sr = SQLBindCol(hstmt, 1, SQL_C_CHAR, row.tab_name, sizeof(row.tab_name), &row.nameLength);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) {
    printf("Error in Binding 1 in OnViewfetchresults");
  }
  // Start fetching records
  while (SQLFetch(hstmt) == SQL_SUCCESS) {
    sprintf(message, "%s\t%s \n", message, row.tab_name);
  }
  printf(message);
  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
}

int main() {
  SQLHENV henv;
  SQLHDBC hdbc;
  SQLHSTMT hstmt;
  SQLRETURN retcode;

  SQLCHAR OutConnStr[255];
  SQLSMALLINT OutConnStrLen;
  SQLCHAR* ConnStrIn = (SQLCHAR *)"DSN=hiveodbcds";

  int mustFreeConnect = 0;
  int mustDisconnect = 0;

  // Allocate environment handle
  retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  CHECK_STATUS_ENV(retcode, (SQLCHAR *)"SQLAllocEnv", henv);


  // Set the ODBC version environment attribute
  if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
    retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER*)3, 0); 
    CHECK_STATUS_ENV(retcode, (SQLCHAR *)"SQLSetEnvAttr", henv);

    // Allocate connection handle
    if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
      retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc); 
      if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
        retcode = SQLAllocConnect(henv, &hdbc);
        mustFreeConnect = CHECK_STATUS_DBC(retcode, (SQLCHAR *)"SQLAllocConnect", henv);
        retcode = SQLDriverConnect(hdbc,
                                   NULL,
                                   (SQLCHAR *)ConnStrIn,
                                   SQL_NTS,
                                   NULL,
                                   0, 
                                   NULL,
                                   SQL_DRIVER_NOPROMPT); 

        mustDisconnect = CHECK_STATUS_DBC(retcode, (SQLCHAR *)"SQLConnect", hdbc);
        // Allocate statement handle
        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {               
          retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt); 
          // Process data
          SQLCHAR * sql = (SQLCHAR *)"show databases;"; 
          viewFetchresults(hdbc, sql);
          printf("OK!\n");  
          if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
            SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
          }
          SQLDisconnect(hdbc);
        }
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
      }
    }
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
  }

  return 1;
}
