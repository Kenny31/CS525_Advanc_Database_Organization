#include "dataStructure.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "tables.h"
#include "expr.h"

int recordSize;

typedef struct ScanManager {
	Expr *scanCond;
	RID rID;
	int NumScans;
} ScanManager;

typedef struct RecordManager {

    int rowCount;
	int emptyPage;
    BM_PageHandle pageHandle;
    BM_BufferPool bufferPool;

} RecordManager;

Schema *createSchema(int numAttr, char **attrNames,DataType *dataTypes, int *typeLength, int keySize, int *keys){
    printf("create schema function starts.");
	RC message;

    if(numAttr < 0 || attrNames == NULL || dataTypes == NULL){
        message = RC_FILE_NOT_FOUND;
    }
    else{
    	Schema *newSchema;
        newSchema = malloc(sizeof(Schema));
        printf("memory allocated");
        newSchema->attrNames = attrNames;
        
        newSchema->dataTypes = dataTypes;
        printf("datatype assigned.");
        newSchema->keyAttrs = keys;
        
        newSchema->keySize = keySize;
        printf("key size allocated");
        newSchema->numAttr = numAttr;
        
        newSchema->typeLength = typeLength;

        printf("new schema created.");
        message = RC_OK;
        return newSchema;

    }
}

extern RC freeSchema (Schema *schema){

	RC msg;
    if (schema == NULL){
        msg = RC_FILE_NOT_FOUND;
    }
    else{
        RecordManager *rmHandle;
        free(schema);
        schema = NULL;
        free(rmHandle);
        msg = RC_OK;
    }
    return msg;
}
