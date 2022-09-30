from TCLIService.ttypes import TOpenSessionReq, TGetTablesReq, TFetchResultsReq,\
  TStatusCode, TGetResultSetMetadataReq, TGetColumnsReq, TType, TTypeId, \
  TExecuteStatementReq, TGetOperationStatusReq, TFetchOrientation, TCloseOperationReq, \
  TCloseSessionReq, TGetSchemasReq, TGetLogReq, TCancelOperationReq, TGetCatalogsReq, TGetInfoReq, TOperationState

from error import Pyhs2Exception
import threading
import time

def get_type(typeDesc):
    for ttype in typeDesc.types:
        if ttype.primitiveEntry is not None:
            return TTypeId._VALUES_TO_NAMES[ttype.primitiveEntry.type]
        elif ttype.mapEntry is not None:
            return ttype.mapEntry
        elif ttype.unionEntry is not None:
            return ttype.unionEntry
        elif ttype.arrayEntry is not None:
            return ttype.arrayEntry
        elif ttype.structEntry is not None:
            return ttype.structEntry
        elif ttype.userDefinedTypeEntry is not None:
            return ttype.userDefinedTypeEntry

def get_value(colValue):
    if colValue.boolVal is not None:
      return colValue.boolVal.value
    elif colValue.byteVal is not None:
      return colValue.byteVal.value
    elif colValue.i16Val is not None:
      return colValue.i16Val.value
    elif colValue.i32Val is not None:
      return colValue.i32Val.value
    elif colValue.i64Val is not None:
      return colValue.i64Val.value
    elif colValue.doubleVal is not None:
      return colValue.doubleVal.value
    elif colValue.stringVal is not None:
      return colValue.stringVal.value

class Cursor(object):
    session = None
    client = None
    operationHandle = None
    hasMoreRows = True
    MAX_BLOCK_SIZE = 10000
    arraysize = 1000
    _currentRecordNum = None
    _currentBlock = None
    _standbyBlock = None
    _blockRequestInProgress = False
    _cursorLock = None

    def __init__(self, _client, sessionHandle):
        self.session = sessionHandle
        self.client = _client
        self._cursorLock = threading.RLock()

    def execute(self, hql):
        self.hasMoreRows = True
        query = TExecuteStatementReq(self.session, statement=hql, confOverlay={})
        res = self.client.ExecuteStatement(query)
        self.operationHandle = res.operationHandle
        if res.status.errorCode is not None:
            raise Pyhs2Exception(res.status.errorCode, res.status.errorMessage)

        while True:
           req = TGetOperationStatusReq(self.operationHandle)
           status = self.client.GetOperationStatus(req)
           if status.status.errorCode is not None:
              raise Pyhs2Exception(status.status.errorCode, status.status.errorMessage)

           if status.operationState == TOperationState.FINISHED_STATE or status.operationState == TOperationState.CLOSED_STATE:
              break
           elif status.operationState == TOperationState.ERROR_STATE:
              raise Pyhs2Exception(status.errorCode, status.errorMessage)
           elif status.operationState == TOperationState.CANCELED_STATE:
              raise Pyhs2Exception(None, "Query was cancelled")
           elif status.operationState == TOperationState.UKNOWN_STATE:
              raise Pyhs2Exception(None, "Unknown query")

           time.sleep(1)

    def fetch(self):
        rows = []
        while self.hasMoreRows:
            rows = rows + self.fetchSet()
        return rows

    def fetchSet(self):
        rows = []
        fetchReq = TFetchResultsReq(operationHandle=self.operationHandle,
                                    orientation=TFetchOrientation.FETCH_NEXT,
                                    maxRows=10000)
        self._fetch(rows, fetchReq)
        return rows

    def _fetchBlock(self):
        """ internal use only.
	 get a block of rows from the server and put in standby block.
         future enhancements:
         (1) locks for multithreaded access (protect from multiple calls)
         (2) allow for prefetch by use of separate thread
        """
        # make sure that another block request is not standing
        if self._blockRequestInProgress :
           # need to wait here before returning... (TODO)
           return

        # make sure another block request has not completed meanwhile
        if self._standbyBlock is not None: 
           return

        self._blockRequestInProgress = True
        fetchReq = TFetchResultsReq(operationHandle=self.operationHandle,
                                    orientation=TFetchOrientation.FETCH_NEXT,
                                    maxRows=self.arraysize)
        self._standbyBlock = self._fetch([],fetchReq)
        self._blockRequestInProgress = False
        return

    def fetchone(self):
        """ fetch a single row. a lock object is used to assure that a single 
	 record will be fetched and all housekeeping done properly in a 
	 multithreaded environment.
         as getting a block is currently synchronous, this also protects 
	 against multiple block requests (but does not protect against 
	 explicit calls to to _fetchBlock())
        """
        self._cursorLock.acquire()

        # if there are available records in current block, 
	# return one and advance counter
        if self._currentBlock is not None and self._currentRecordNum < len(self._currentBlock):
           x = self._currentRecordNum
           self._currentRecordNum += 1
           self._cursorLock.release()
           return self._currentBlock[x]

        # if no standby block is waiting, fetch a block
        if self._standbyBlock is None:
           # TODO - make sure exceptions due to problems in getting the block 
	   # of records from the server are handled properly
           self._fetchBlock()

        # if we still do not have a standby block (or it is empty), 
	# return None - no more data is available
        if self._standbyBlock is None or len(self._standbyBlock)==0:
           self._cursorLock.release()
           return None

        #  move the standby to current
        self._currentBlock = self._standbyBlock 
        self._standbyBlock = None
        self._currentRecordNum = 1

        # return the first record
        self._cursorLock.release()
        return self._currentBlock[0]

    def fetchmany(self,size=-1):
        """ return a sequential set of records. This is guaranteed by locking, 
	 so that no other thread can grab a few records while a set is fetched.
         this has the side effect that other threads may have to wait for 
         an arbitrary long time for the completion of the current request.
        """
        self._cursorLock.acquire()

        # default value (or just checking that someone did not put a ridiculous size)
        if size < 0 or size > MAX_BLOCK_SIZE:
           size = self.arraysize
        recs = []
        for i in range(0,size):
            recs.append(self.fetchone())

        self._cursorLock.release()
        return recs

    def fetchall(self):
        """ returns the remainder of records from the query. This is 
	 guaranteed by locking, so that no other thread can grab a few records 
	 while the set is fetched. This has the side effect that other threads 
	 may have to wait for an arbitrary long time until this query is done 
	 before they can return (obviously with None).
        """
        self._cursorLock.acquire()

        recs = []
        while True:
            rec = self.fetchone()
            if rec is None:
               break
            recs.append(rec)

        self._cursorLock.release()
        return recs

    def __iter__(self):
        """ returns an iterator object. no special code needed here. """
        return self

    def next(self):
        """ iterator-protocol for fetch next row. """
        row = self.fetchone()
        if row is None:
           raise StopIteration 
        return row

    def getSchema(self):
        if self.operationHandle:
            req = TGetResultSetMetadataReq(self.operationHandle)
            res = self.client.GetResultSetMetadata(req)
            if res.schema is not None:
                cols = []
                for c in self.client.GetResultSetMetadata(req).schema.columns:
                    col = {}
                    col['type'] = get_type(c.typeDesc)
                    col['columnName'] = c.columnName
                    col['comment'] = c.comment
                    cols.append(col)
                return cols
        return None

    def getDatabases(self):
        req = TGetSchemasReq(self.session)
        res = self.client.GetSchemas(req)
        self.operationHandle = res.operationHandle
        if res.status.errorCode is not None:
            raise Pyhs2Exception(res.status.errorCode, res.status.errorMessage)
        return self.fetch()

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.close()

    def _fetch(self, rows, fetchReq):
        resultsRes = self.client.FetchResults(fetchReq)
        for row in resultsRes.results.rows:
            rowData= []
            for i, col in enumerate(row.colVals):
                rowData.append(get_value(col))
            rows.append(rowData)
        if len(resultsRes.results.rows) == 0:
            self.hasMoreRows = False
        return rows

    def close(self):
        if self.operationHandle is not None:
            req = TCloseOperationReq(operationHandle=self.operationHandle)
            self.client.CloseOperation(req) 
