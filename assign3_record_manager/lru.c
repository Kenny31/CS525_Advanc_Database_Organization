#include "fifo.c"

void LRUStrategy(BM_BufferPool *const bm, BM_BufferPoolManager *bmgr){
    RC message;
    int i;
    int index;
    int minimum;
    SM_FileHandle fHandle;
    BM_BufferPoolManager *tempMgr = (BM_BufferPoolManager *) bm->mgmtData;
    i=0;
    index = 0;
    minimum = tempMgr[0].lastAccessedTime;
    for(i=0; i<bm->numPages; i++){
        if(tempMgr[i].fixCount != 0){
            message = RC_FILE_HANDLE_NOT_INIT;
        }
        else{
            minimum = tempMgr[i].lastAccessedTime;
            index = i;
            break;
        }
    }
    for(i = index+1; i<bm->numPages; i++){
        if(tempMgr[i].lastAccessedTime >= minimum){
            message = RC_FILE_HANDLE_NOT_INIT;            
        }
        else{
            minimum = tempMgr[i].lastAccessedTime;
            index = i;
        }
    }
    if(tempMgr[index].isDirtyFlag == true){
        RC isOpen;
        isOpen = openPageFile(bm->pageFile, &fHandle);
        if(isOpen == RC_OK){
            message = writeBlock(tempMgr[index].pageSerial, &fHandle, tempMgr[index].data);
            writeIO++;
        }
        else{
            message = RC_READ_NON_EXISTING_PAGE;
        }
    }
    tempMgr[index].lastAccessedTime = ++frameCount;
    tempMgr[index].data = bmgr->data;
    printf("%d", &tempMgr[index].lastAccessedTime);
    tempMgr[index].pageSerial = bmgr->pageSerial;
    tempMgr[index].isDirtyFlag = bmgr->isDirtyFlag;
    printf("%d", &tempMgr[index].isDirtyFlag);
    tempMgr[index].fixCount = bmgr->fixCount;
    printf("%d", &tempMgr[index].fixCount);
}