#include "lru.c"



int readIO;
int writeIO;
int frameCount;

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData){
    RC message;
    if(pageFileName == NULL){
        message = RC_FILE_NOT_FOUND;
        return message;
    }
    else{
        writeIO = 0;
        frameCount = 0;
        readIO = 0;
        if(numPages <= 0){
            message = RC_READ_NON_EXISTING_PAGE;
            return message;
        }
        else{
            // if(stratData == NULL){
            //     message = RC_FILE_HANDLE_NOT_INIT;
            //     return message;
            // }
            //else{
                BM_BufferPoolManager *bmgr = malloc(sizeof(BM_BufferPoolManager) * numPages);
                for(int i=0; i<numPages; i++){
                    bmgr[i].isDirtyFlag = FALSE;
                    bmgr[i].fixCount = 0;
                    bmgr[i].pageSerial = NO_PAGE;
                    bmgr[i].data = NULL;
                    bmgr[i].lastAccessedTime = 0;
      
                }

                bm->numPages = numPages;
                bm->pageFile = (char *) pageFileName;
                bm->strategy = strategy;
                bm->mgmtData = bmgr;
                
                message = RC_OK;
                return message;
            //}
        }
    }
}

RC shutdownBufferPool(BM_BufferPool *const bm){
    RC message;
    BM_BufferPoolManager *bmgr = (BM_BufferPoolManager *) bm->mgmtData;

    if(bmgr == NULL){
        message = RC_FILE_NOT_FOUND;
        return message;
    }
    else{
        RC temp = forceFlushPool(bm);
        if(temp == RC_OK){

            int i=0;
            while (i < bm->numPages)
            {
                if(bmgr[i].fixCount != 0){
                    message = RC_FILE_NOT_FOUND;
                    return message;
                }
                i++;
            }
            
            free(bmgr);

            bm->pageFile = NULL;
            bm->strategy = -1;
            bm->mgmtData = NULL;
            message = RC_OK;
            return message;
        }
        else{
            return temp;
        }    
    }
}

RC forceFlushPool(BM_BufferPool *const bm){
    RC message;
    BM_BufferPoolManager *bmgr = (BM_BufferPoolManager *) bm->mgmtData;
    if(bmgr == NULL){
        message = RC_FILE_HANDLE_NOT_INIT;
        return message;
    }
    else{
        if(bmgr->fixCount < 0){
            message = RC_FILE_NOT_FOUND;
            return message;
        }
        else{
            SM_FileHandle fHandle;
            for(int i=0; i < bm->numPages ; i++){
                if(bmgr[i].isDirtyFlag == true && bmgr[i].fixCount == 0){
                    // need to add exception handling
                    RC isOpen = openPageFile(bm->pageFile, &fHandle);
                    if(isOpen == RC_OK){
                        RC isWriteSuccess;
                        isWriteSuccess = writeBlock(bmgr[i].pageSerial, &fHandle, bmgr[i].data);
                        if(isWriteSuccess == RC_OK){
                            //closePageFile(&fHandle);
                            bmgr[i].isDirtyFlag = false;
                            writeIO++;
                        }
                        else{
                            message = RC_WRITE_FAILED;
                        }
                    }
                    else{
                        message = RC_FILE_NOT_FOUND;
                    }
                }
            }
        message = RC_OK;
        return message;
        }
    }
}

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    RC message;
    BM_BufferPoolManager *bmgr = (BM_BufferPoolManager *) bm->mgmtData;
    if(bmgr == NULL){
        message = RC_FILE_NOT_FOUND;
        return message;
    }
    else{
        if(page == NULL){
            message = RC_READ_NON_EXISTING_PAGE;
            return message;
        }
        else{
            int pageNumber = page->pageNum;
            if(pageNumber < 0){
                message = RC_READ_NON_EXISTING_PAGE;
                return message;
            }
            else{
                for (int i = 0; i < bm->numPages; i++){
                    if(bmgr[i].pageSerial == page->pageNum){
                        bmgr[i].isDirtyFlag = true;
                        message = RC_OK;
                        break;
                    }
                }
            }
        }
    } 
    return message;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
    RC message;
    BM_BufferPoolManager *bmgr = (BM_BufferPoolManager *) bm->mgmtData;
    if(bmgr == NULL){
        message = RC_FILE_NOT_FOUND;
        return message;
    }
    else{
        if(page == NULL){
            message = RC_READ_NON_EXISTING_PAGE;
            return message;
        }
        else{
            int pageNumber = page->pageNum;
            if(pageNumber < 0){
                message = RC_READ_NON_EXISTING_PAGE;
                return message;
            }
            else{
                for (int i = 0; i < bm->numPages; i++){
                    if(bmgr[i].pageSerial == pageNumber){
                        // if(bmgr[i].fixCount == 0){
                        //     message = RC_READ_NON_EXISTING_PAGE;
                        //     return message;
                        // }
                        // else{
                            bmgr[i].fixCount--;
                            break;
                        //}
                    }
                    // else{
                    //     message = RC_FILE_NOT_FOUND;
                    //     return message;
                    // }
                }
                message = RC_OK;
                return message;
            }
        }
    }
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
    RC message;
    SM_FileHandle fileInfo;
    BM_BufferPoolManager *bmgr = (BM_BufferPoolManager *) bm->mgmtData;
    if(bmgr == NULL){
        message = RC_FILE_NOT_FOUND;
        return message;
    }
    else
    {
        if(page == NULL){
            message = RC_READ_NON_EXISTING_PAGE;
            return message;
        }
        else{
            int pageNumber = page->pageNum;
            if(pageNumber < 0){
                message = RC_READ_NON_EXISTING_PAGE;
                return message;
            }
            else{
                for (int i = 0; i < bm->numPages; i++){
                    if(bmgr[i].pageSerial == pageNumber){
                        // if(bmgr[i].isDirtyFlag == FALSE){
                        //     message= RC_OK;
                        //     return message;
                        // }
                            message = openPageFile(bm->pageFile, &fileInfo);
                            message = writeBlock(bmgr[i].pageSerial, &fileInfo, bmgr[i].data);
                            
                            if(message == RC_OK){
                                bmgr[i].isDirtyFlag= FALSE;
                                writeIO+=1;
                                break;
                            }
                    }
                    // return message;
                }
            }
        }   
    return message;
    }
} 

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
    RC message = RC_OK;
    BM_BufferPoolManager *bmgr = (BM_BufferPoolManager *) bm->mgmtData;
    
    if(bmgr[0].pageSerial == NO_PAGE){
        SM_FileHandle fHandle;
        RC isOpen;
        RC isReadSuccess;
        isOpen = openPageFile(bm->pageFile, &fHandle);
        if(isOpen == RC_OK){
            bmgr[0].data = (SM_PageHandle ) malloc(PAGE_SIZE);
            RC ensured = ensureCapacity(pageNum+1, &fHandle);
            if(ensured == RC_OK){
                isReadSuccess = readBlock(pageNum, &fHandle, bmgr[0].data);
                if(isReadSuccess == RC_OK){
                    readIO = 0;
                    frameCount = 0;
                    bmgr[0].pageSerial = pageNum;
                    //bmgr[0].isDirtyFlag = false;
                    
                    bmgr[0].lastAccessedTime = frameCount;
                    bmgr[0].fixCount++;
                    page->pageNum = pageNum;
                    page->data = bmgr[0].data;
                    RC isClosed = closePageFile( &fHandle);
                    if(isClosed == RC_OK){
                        message = RC_OK;
                        //return message;
                    }
                    else{
                        message = RC_FILE_NOT_FOUND;
                    }
                }
                else{
                    message = RC_READ_NON_EXISTING_PAGE;
                }
            }
            else{
                message = RC_FILE_NOT_FOUND;
            }
        }
        else{
            message = RC_READ_NON_EXISTING_PAGE;
        }
    }
    else{
        bool hasCapacity = true;
        
        for(int i=0; i<bm->numPages; i++){
            RC isEnsured;
            RC readSucess;
            if(bmgr[i].pageSerial == NO_PAGE){
                SM_FileHandle fHandle;
                RC isOpen = openPageFile(bm->pageFile, &fHandle);
                if(isOpen == RC_OK){
                    bmgr[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
                    isEnsured = ensureCapacity(pageNum+1, &fHandle);
                    if(isEnsured == RC_OK){
                        readSucess = readBlock(pageNum, &fHandle, bmgr[i].data);
                        if(readSucess == RC_OK){
                            readIO+=1;
                            frameCount+=1;
                            bmgr[i].pageSerial = pageNum;
                            bmgr[i].fixCount = 1;
                            hasCapacity = false;
                                                            
                            page->pageNum = pageNum;
                            page->data = bmgr[i].data;
                            if(bm->strategy == RS_LRU){
                                bmgr[i].lastAccessedTime = frameCount;
                            }
                            message = closePageFile(&fHandle);
                            if(message == RC_OK){
                                break;
                            }
                            else{
                                message = RC_FILE_NOT_FOUND;
                            }
                        }
                        else{
                            message = RC_READ_NON_EXISTING_PAGE;
                        }
                    }
                    else{
                        message = RC_FILE_NOT_FOUND;
                    }
                }
                else{
                    message = RC_FILE_NOT_FOUND;
                }
            } 
            else{
                if(bmgr[i].pageSerial == pageNum){
                    // bmgr[i].pageSerial = page->pageNum;
                    // bmgr[i].data = page->data;
                    bmgr[i].fixCount++;
                    hasCapacity = false;
                    frameCount+=1;
                    if(bm->strategy == RS_LRU){
                        bmgr[i].lastAccessedTime = frameCount;
                    }
                    page->pageNum = pageNum;
                    page->data = bmgr[i].data;
                    break;
                }
            }
        }
        if(hasCapacity == true){
            RC isOpen;
            RC isEnsured;
            RC readProperly;
            SM_FileHandle fHandle;
            BM_BufferPoolManager *temp = (BM_BufferPoolManager *) malloc(sizeof(BM_BufferPoolManager));
            isOpen = openPageFile(bm->pageFile, &fHandle);
            if(isOpen == RC_OK){
                temp->data = (SM_PageHandle) malloc(PAGE_SIZE);
                isEnsured = ensureCapacity(pageNum+1, &fHandle);
                if(isEnsured != RC_OK){
                    message = RC_FILE_NOT_FOUND;
                }
                else{
                    message = readBlock(pageNum, &fHandle, temp->data);
                    temp->pageSerial = pageNum;
                    temp->fixCount = 1;
                    temp->isDirtyFlag = false;
                    frameCount++;
                    readIO++;
                    if(bm->strategy == RS_LRU){
                        temp->lastAccessedTime = frameCount;
                    }
                    page->pageNum = pageNum;
                    page->data = temp->data;
                    if(bm->strategy == RS_FIFO){
                        FIFOStrategy(bm, temp);
                    }
                    else if(bm->strategy == RS_LRU){
                        LRUStrategy(bm, temp);
                    }
                    else{
                        printf("Other strategies not implemented.");
                    }
                }
                
            }
            else{
                message = RC_FILE_NOT_FOUND;
            }
        }
    }
    return message;
}

void isBufferFull(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum, BM_BufferPoolManager *bmgr){
    RC message;
    SM_FileHandle fHandle;
    BM_BufferPoolManager *temp = (BM_BufferPoolManager *) malloc(sizeof(BM_BufferPoolManager));
    // message = openPageFile(bm->pageFile, &fHandle);
    // //if(isOpen == RC_OK){
    // temp->data = (SM_PageHandle) malloc(PAGE_SIZE);
    // message = ensureCapacity(pageNum+1, &fHandle);
    // message = readBlock(pageNum, &fHandle, temp->data);
    if(bm->strategy == RS_LRU){
        temp->lastAccessedTime = frameCount;
    }
    temp->isDirtyFlag = false;
    temp->fixCount = 1;
    temp->pageSerial = pageNum;
    readIO++;
    frameCount++;
    
    page->pageNum = pageNum;
    page->data = temp->data;
    if(bm->strategy == RS_FIFO){
        FIFOStrategy(bm, temp);
    }
    else if(bm->strategy == RS_LRU){
        LRUStrategy(bm, temp);
    }
    else{
        printf("Other strategies not implemented.");
    }
}



PageNumber *getFrameContents (BM_BufferPool *const bm){
    BM_BufferPoolManager *bmgr = (BM_BufferPoolManager *) bm->mgmtData;
    PageNumber *pageNumber = malloc(sizeof(PageNumber)*bm->numPages);

    for(int i=0; i<bm->numPages; i++){
        if(bmgr[i].pageSerial == -1){
            pageNumber[i] = -1;
        }
        else{
            pageNumber[i] = bmgr[i].pageSerial;
        }
    }
    return pageNumber;
}

bool *getDirtyFlags (BM_BufferPool *const bm){
    BM_BufferPoolManager *bmgr = (BM_BufferPoolManager *) bm->mgmtData;
    bool *dirtyFlag = malloc(sizeof(bool) * bm->numPages);
    if(bmgr == NULL){
        return false;
    }
    else{
        for(int i=0; i<bm->numPages; i++){
            if(bmgr[i].isDirtyFlag != 0){
                dirtyFlag[i] = true;
            }
            else{
                dirtyFlag[i] = false;
            }
        }
        return dirtyFlag;
    }
}

int *getFixCounts (BM_BufferPool *const bm){
    BM_BufferPoolManager *bmgr = (BM_BufferPoolManager *) bm->mgmtData;
    int *fixedCount = malloc(sizeof(int) * bm->numPages);
    if(bmgr == NULL){
        return false;
    }
    else{
        for(int i=0; i<bm->numPages; i++){
            if(bmgr[i].fixCount == -1){
                fixedCount[i] = 0;
            }
            else{
                fixedCount[i] = bmgr[i].fixCount;
            }
        }
        return fixedCount;
    }
}

int getNumReadIO (BM_BufferPool *const bm){
    BM_BufferPoolManager *bmgr = (BM_BufferPoolManager *) bm->mgmtData;
    if(bmgr == NULL){
        return false;
    }
    else{
        int readOperations = readIO + 1;
        return readOperations;
    }
}

int getNumWriteIO (BM_BufferPool *const bm){
    BM_BufferPoolManager *bmgr = (BM_BufferPoolManager *) bm->mgmtData;
    if(bmgr == NULL){
        return false;
    }
    else{
        int writeOperations = writeIO;
        return writeOperations;
    }
}