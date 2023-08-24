#include "dataStructure.h"


void FIFOStrategy(BM_BufferPool *const bm, BM_BufferPoolManager *bmgr){
    int frontIndex;
    int rearIndex;
    BM_BufferPoolManager *tempMgr = (BM_BufferPoolManager * ) bm->mgmtData;
    RC message;
    SM_FileHandle fHandle;
    
    frontIndex = readIO % bm->numPages;
    rearIndex = frontIndex % bm->numPages;

    for(int i=0; i<bm->numPages; i++){
        if(tempMgr[frontIndex].fixCount == 0){
            if(tempMgr[frontIndex].isDirtyFlag == true){
                RC isOpen = openPageFile(bm->pageFile, &fHandle);
                if(isOpen == RC_OK){
                    message = writeBlock(tempMgr[frontIndex].pageSerial, &fHandle, tempMgr[frontIndex].data);
                    if(message == RC_OK){
                        writeIO++;
                    }
                }
                else{
                    message = RC_READ_NON_EXISTING_PAGE;
                }
            }
            printf("success point");
            tempMgr[frontIndex].isDirtyFlag = bmgr->isDirtyFlag;
            printf("success point");
            tempMgr[frontIndex].fixCount = bmgr->fixCount;
            printf("success point");
            tempMgr[frontIndex].data = bmgr->data;
            printf("success point");
            tempMgr[frontIndex].pageSerial = bmgr->pageSerial;
            //tempMgr[frontIndex].lastAccessedTime = bmgr->lastAccessedTime;
            break;
        }
        else{
            frontIndex++;
            if(rearIndex != 0){
                frontIndex = frontIndex;
            }
            else{
                frontIndex = 0;
            }
        }
    }
}