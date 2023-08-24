#include "crudRecord.c"


int recordSize;

RecordManager *recordManager;


RC initRecordManager (void *mgmtData) {
	RC msg;
    RecordManager *rmHandle = malloc(sizeof(RecordManager));
    initStorageManager();
        msg = RC_OK;
   
    return msg;
}


RC shutdownRecordManager() {

    RC msg;
    free(recordManager);
    msg = RC_OK;
    return msg;
}

extern RC createTable (char *name, Schema *schema)
{
	char data[4096];
    recordManager = (RecordManager*) calloc(sizeof(RecordManager), 1);
    char *pHandle;

    RC isBPInit = initBufferPool(&recordManager->bufferPool, name, 100, 1, NULL);
    if(isBPInit == RC_OK){
        pHandle = data;
        *(int*)pHandle = 0;
        pHandle += 4;
        *(int*)pHandle = 1;
        pHandle += 4;
        *(int*)pHandle = schema->numAttr;
        pHandle += 4;
        *(int*)pHandle = schema->keySize;
        pHandle += 4;
        for(int k = 0; k < schema->numAttr; k++){
            *(int*)pHandle = (int) schema->typeLength[k];
            pHandle += 4;
            *(int*)pHandle = (int)schema->dataTypes[k];
            pHandle += 4;
            strncpy(pHandle, schema->attrNames[k], 15);
            pHandle += 15;
        }
        SM_FileHandle fileHandle;
        if(((createPageFile(name)) != RC_OK) || ((openPageFile(name, &fileHandle)) != RC_OK) || ((writeBlock(0, &fileHandle, data)) != RC_OK))
            return RC_FILE_NOT_FOUND;

        return RC_OK;
    }
}


extern RC openTable(RM_TableData *rel, char *name)
{
	Schema *schema;
	schema = (Schema*) calloc(sizeof(Schema),1);  

	int attCount; 
	rel->mgmtData = recordManager;
	
	rel->name = name;
	pinPage(&recordManager->bufferPool, &recordManager->pageHandle, 0);
	
	int intSize =4;
	SM_PageHandle pageHandle = (char*) recordManager->pageHandle.data;	
	pageHandle += intSize;

	recordManager->emptyPage= *(int*) pageHandle;
	pageHandle += intSize;
	
	attCount = *(int*)pageHandle;
	pageHandle += intSize;
	
	schema->numAttr = attCount;
	schema->attrNames = (char**) calloc(attCount ,sizeof(char*));
	schema->typeLength = (int*) calloc(attCount,intSize);
	
	schema->dataTypes = (DataType*) malloc(attCount* sizeof(DataType));

    for (int iter = 0; iter < attCount; iter++) {
        schema->attrNames[iter] = (char*)malloc(sizeof(char) * 15);
    }


    for (int iterval = 0; iterval < schema->numAttr; iterval++) {
        char* attrName = schema->attrNames[iterval];

        for (int index = 1; index < 16; index++) {
            while (index > 0){
                attrName[index] = *(pageHandle++);
                break;
            }
        }
        schema->typeLength[iterval] = *(int*)pageHandle;
        while(iterval != -1){
            schema->dataTypes[iterval] = *(int*)pageHandle;
            pageHandle += 8;
            break;
        }
    }

	rel->schema = schema;	
    if(unpinPage(&recordManager->bufferPool, &recordManager->pageHandle)){
        if(forcePage(&recordManager->bufferPool, &recordManager->pageHandle)){
            return RC_OK;
        }
    }
	return RC_OK;
}


extern RC closeTable(RM_TableData *rel) {
	RC msg;
    if(rel == NULL){
        msg = RC_FILE_NOT_FOUND;
    }
    else{
        RecordManager *rmHandle;
        rmHandle = rel->mgmtData;
        if(rmHandle->bufferPool.mgmtData == NULL){
            msg = RC_FILE_HANDLE_NOT_INIT;
        }
        else{
            shutdownBufferPool(&rmHandle->bufferPool);
			msg = RC_OK;
        }
    }
    return msg;
}


extern RC deleteTable(char *name) {
	RC msg;
    if(name == NULL){
        msg = RC_FILE_NOT_FOUND;
    }
    else{
        destroyPageFile(&name);
        msg = RC_OK;
    }
    return msg;
}

extern int getNumTuples(RM_TableData *rel) {

   int tupleCount;
    if(rel == NULL){
        tupleCount = -1;
    }
    else{
        RecordManager *rmHandle;
        rmHandle = rel->mgmtData;
        tupleCount = rmHandle->rowCount;
    }
    return tupleCount;
}

RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {

	RC code = openTable(rel, "ScanTable");
	if(code != RC_OK) {
		printf("openTbale not working properly.");
		return code;
	}
    if(cond == NULL){
		printf("Null in condition in arguments");
        return code;
    }
    ScanManager *scanMgr = (ScanManager*) malloc(sizeof(ScanManager));
	scanMgr->NumScans = 0;
    scanMgr->rID.page = 1;
	scanMgr->rID.slot = 0;
    scanMgr->scanCond = cond;
	scan->mgmtData = scanMgr;
    scan->rel= rel;

	return code;
}

RC next (RM_ScanHandle *scan, Record *record) {
	RC code = RC_OK;
	ScanManager *scanMgr =  scan->mgmtData;
	char *slot;
	Value *result = (Value *) malloc(sizeof(Value));
	while(getNumTuples(scan->rel) > (scanMgr->NumScans)){  
		if(scanMgr->NumScans > 0){
			scanMgr->rID.slot++;
            int numOfRecords = PAGE_SIZE / recordSize;
			if(numOfRecords <= scanMgr->rID.slot) {
				scanMgr->rID.page+=1;
				scanMgr->rID.slot = 0;
			}
		}
		code = getRecord(scan->rel, scanMgr->rID, record);
		if(code != RC_OK)
		{
			printf("getRecord method not working properly.");
			return code;
		}
		scanMgr->NumScans+=1;
		while(scanMgr->scanCond == NULL){
			result->v.boolV = TRUE;
            break;
		}
		while(scanMgr->scanCond != NULL){
			code = evalExpr(record, scan->rel->schema, scanMgr->scanCond, &result);
			if(code != RC_OK){
				return code;
			} 
            break;
		}
		while(result->v.boolV == TRUE) {
			return code;
		}
	}
	scanMgr->NumScans = 0;
	scanMgr->rID.slot = 0;
    scanMgr->rID.page = 1;
	
	code = RC_RM_NO_MORE_TUPLES;
	return code;
}

RC closeScan (RM_ScanHandle *scan) {

	RC msg;
    if(scan == NULL){
        msg = RC_FILE_NOT_FOUND;
    }
    else{
        free(scan->mgmtData);
        scan->mgmtData = NULL;
        msg = RC_OK;
    }
    return msg;
}

extern int getRecordSize(Schema *schema){

	RC msg;
    int sizeCell = 0;
    if(schema == NULL){
        msg = RC_FILE_NOT_FOUND;
    }
    else{
        int sizeInteger = sizeof(int);
        int sizeFloat = sizeof(float);
        int sizeBool = sizeof(bool);

        for(int i=0; i<schema->numAttr; i++){
            switch(schema->dataTypes[i]){
                case DT_STRING :
                sizeCell += schema->typeLength[i];
                break;

                case DT_BOOL :
                sizeCell += sizeBool;
                break;

                case DT_FLOAT :
                sizeCell += sizeFloat;
                break;

                case DT_INT :
                sizeCell += sizeInteger;
                break;
            }
        }
        msg = RC_OK;
        return sizeCell;
    }
}

extern RC createRecord (Record **record, Schema *schema)
{
	Record *newRecord = (Record*) calloc(sizeof(Record),1);
    newRecord->data= (char*) calloc(getRecordSize(schema),1);
    newRecord->id.slot = -1;
    newRecord->id.page = -1;

    char *dataPointer = newRecord->data;

    *(++dataPointer) = '\0';
    *dataPointer = '-';
    *record = newRecord;
    printf("null at EOF");

	return RC_OK;
}

extern RC freeRecord (Record *record)
{
	RC msg;
    if(record == NULL){
        msg = RC_FILE_NOT_FOUND;
    }
    else{
        free(record);
		record = NULL;
        msg = RC_OK;
    }
    return msg;
}

extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	RC msg;
    if(record == NULL || schema == NULL || value == NULL){
        msg = RC_FILE_NOT_FOUND;
    }
    if(attrNum < 0){
        msg = RC_FILE_NOT_FOUND;
    }
    else{
        int offset = 1;
        
        int sizeInteger = sizeof(int);
        int sizeFloat = sizeof(float);
        int sizeBool = sizeof(bool);

        for(int i=0; i<attrNum; i++){
            switch(schema->dataTypes[i]){
                case DT_STRING :
                offset += schema->typeLength[i];
                break;

                case DT_BOOL :
                offset += sizeBool;
                break;

                case DT_FLOAT :
                offset += sizeFloat;
                break;

                case DT_INT :
                offset += sizeInteger;
                break;
            }
        }

        char *data = record->data;

        data = data + offset;
        
        DataType dataType = schema->dataTypes[attrNum];

        Value *val = (Value*) malloc(sizeof(Value));

        if(attrNum == 1){
    	    schema->dataTypes[attrNum] = 1;
        }

        if(schema->dataTypes[attrNum] == DT_STRING){
           
            val->v.stringV = (char *) malloc(4);
            strncpy(val->v.stringV, data, 4);
            val->v.stringV[4] = '\0';
            val->dt = 1;
        }
         if(schema->dataTypes[attrNum] == DT_BOOL){
           
            bool contains;
            memcpy(&contains, data, sizeBool);
            val->v.floatV = contains;
            val->dt = DT_BOOL;
        }
         if(schema->dataTypes[attrNum] == DT_FLOAT){
            
            float contains;
            memcpy(&contains, data, sizeFloat);
            val->v.floatV = contains;
            val->dt = 2;
        }
        if(schema->dataTypes[attrNum] == DT_INT){
            
            int contains;
            memcpy(&contains, data, sizeInteger);
            val->v.intV = contains;
            val->dt = 0;

        }

        *value = val;

        msg = RC_OK;
    }
    return msg;
}

extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	RC msg;
    if(record == NULL || schema == NULL || value == NULL){
        msg = RC_FILE_NOT_FOUND;
    }
    if(attrNum < 0){
        msg = RC_FILE_NOT_FOUND;
    }
    else{
        int offset = 1;
        
        int sizeInteger = sizeof(int);
        int sizeFloat = sizeof(float);
        int sizeBool = sizeof(bool);

        for(int i=0; i<attrNum; i++){
            switch(schema->dataTypes[i]){
                case DT_STRING :
                offset += schema->typeLength[i];
                break;

                case DT_BOOL :
                offset += sizeBool;
                break;

                case DT_FLOAT :
                offset += sizeFloat;
                break;

                case DT_INT :
                offset += sizeInteger;
                break;
            }
        }

        char *data = record->data;
        data = data + offset;

        DataType dataType = schema->dataTypes[attrNum];

        if(schema->dataTypes[attrNum] == DT_BOOL){
            *(bool *) data = value->v.intV;
		    data += sizeBool;
        }
        if(schema->dataTypes[attrNum] == DT_FLOAT){
            *(float *) data = value->v.floatV;
            data = data+ sizeFloat;
        }
        if(schema->dataTypes[attrNum] == DT_STRING){
            strncpy(data, value->v.stringV, schema->typeLength[attrNum]);
            data += schema->typeLength[attrNum];
        }
         if(schema->dataTypes[attrNum] == DT_INT){
            *(int *) data = value->v.intV;
		    data += sizeInteger;
        }
        msg = RC_OK;
    }
    return msg;
}