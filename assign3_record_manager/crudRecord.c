#include "schemas.c"


RC insertRecord (RM_TableData *rel, Record *record) {
	RecordManager *recordmgr = rel->mgmtData;	
	char *space;
	recordSize = getRecordSize(rel->schema);
    bool checkFirst = false;
    bool assignedSlots = false;
    record->id.page = recordmgr->emptyPage;
        while(assignedSlots == false) {
        if(checkFirst == true) {
             unpinPage(&recordmgr->bufferPool, &recordmgr->pageHandle);
             record->id.page++;
        }
        pinPage(&recordmgr->bufferPool, &recordmgr->pageHandle, record->id.page);
        space = recordmgr->pageHandle.data;
        for (int i = 0; i < 4096 / getRecordSize(rel->schema); i++) {
            if (space[i * getRecordSize(rel->schema)] == '+') {
                
                continue;;
            }
            else{
                record->id.slot = i;
                assignedSlots = true;
                break;
            }
        }
		checkFirst = true;
    } 	
	space += (record->id.slot * getRecordSize(rel->schema));
	*space = '+';
	memcpy(++space, record->data + 1, getRecordSize(rel->schema) - 1);
	markDirty(&recordmgr->bufferPool, &recordmgr->pageHandle);
	unpinPage(&recordmgr->bufferPool, &recordmgr->pageHandle);
	
	recordmgr->rowCount+=1;
	pinPage(&recordmgr->bufferPool, &recordmgr->pageHandle, 0);

	rel->mgmtData = recordmgr;
	return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id) {

	RecordManager *recordManager = rel->mgmtData;
	pinPage(&recordManager->bufferPool, &recordManager->pageHandle, id.page);
	recordManager->rowCount = id.page;
    char *data = recordManager->pageHandle.data;
    *data = '-';
    data += (id.slot * getRecordSize(rel->schema));

    markDirty(&recordManager->bufferPool, &recordManager->pageHandle);
    if(unpinPage(&recordManager->bufferPool, &recordManager->pageHandle)){
        return RC_OK;
    }

    return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record) {
	RecordManager *recordManager = rel->mgmtData;
	char *data;
	pinPage(&recordManager->bufferPool, &recordManager->pageHandle, record->id.page);
		
    data = recordManager->pageHandle.data;
    *data = '+';
    data += (record->id.slot * getRecordSize(rel->schema));
    
    memcpy(++data, record->data + 1, getRecordSize(rel->schema) - 1 );
    markDirty(&recordManager->bufferPool, &recordManager->pageHandle);
    if(unpinPage(&recordManager->bufferPool, &recordManager->pageHandle)){
        return RC_OK;
    }
    return RC_OK;	
}

RC getRecord (RM_TableData *rel, RID id, Record *record) {

	RecordManager *recordmgr = rel->mgmtData;
    char *data;
    char *slot;

    pinPage(&recordmgr->bufferPool, &recordmgr->pageHandle, id.page);
	slot = recordmgr->pageHandle.data;
	slot += id.slot *  recordSize;
	
		record->id.page = id.page;
		record->id.slot = id.slot;
		data = record->data;
		memcpy(++data, slot + 1,  recordSize-1);
	
    if(unpinPage(&recordmgr->bufferPool, &recordmgr->pageHandle)){
        return RC_OK;
    }
	return RC_OK;
}