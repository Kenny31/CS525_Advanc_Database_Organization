#include "dberror.h"
#include "storage_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern void initStorageManager (){
    
}

extern RC createPageFile(char *fileName)
{
    RC msg;
    FILE *fp = fopen(fileName, "w");
    char *blankPage;
    if (fp == NULL)
    {
        msg = RC_FILE_NOT_FOUND;
        return msg;
    }
    else
    {
        blankPage = malloc(PAGE_SIZE * sizeof(char));
        fwrite(blankPage, sizeof(char), 4096, fp);
        free(blankPage);
        fclose(fp);
        msg = RC_OK;
    }
    return msg;
}

extern RC openPageFile (char * fileName , SM_FileHandle * fHandle ){
    RC msg;
    SM_PageHandle *page;
    FILE *fp;
    fp = fopen(fileName, "r+");

    if (fp != NULL)
    {
        // page->fHandle = fHandle;
        //fseek(fp, 0, SEEK_END);
        int fileSize = ftell(fp);
        fHandle->fileName = fileName;
        fHandle->totalNumPages = fileSize+1;
        fHandle->curPagePos = fseek(fp, 0, SEEK_SET);
        fHandle->mgmtInfo = NULL;
        msg = RC_OK;
        fclose(fp);
        return msg;
    }
    else
    {
        msg = RC_FILE_NOT_FOUND;
        return msg;
    }

    return msg;
}

extern RC closePageFile (SM_FileHandle * fHandle ){
    RC msg;
    FILE *fp = fopen(fHandle->fileName, "r");
    if (fHandle == NULL)
    {
        msg = RC_FILE_HANDLE_NOT_INIT;
    }
    if (fp == NULL)
    {
        msg = RC_FILE_NOT_FOUND;
    }

    int temp = fclose(fp);
    printf("%d", temp);
    // page->fHandle = NULL;
    fHandle->fileName = NULL;
    fHandle->totalNumPages = 0;
    fHandle->curPagePos = 0;
    fHandle->mgmtInfo = NULL;

    msg = RC_OK;

    return msg;

}

extern RC destroyPageFile (char * fileName ){
    RC msg;
    int success = -1;
    if(fileName!=NULL){
        success = remove(fileName);
        if(success == 0){
            msg = RC_OK;
        }
        else{
            msg = RC_FILE_NOT_FOUND;   
        }
    
    }
    else{
        msg = RC_FILE_NOT_FOUND;
    }
    return msg;
}

extern RC readBlock (int pageNum , SM_FileHandle * fHandle , SM_PageHandle memPage ){
    RC msg;
    if(fHandle == NULL){
        msg = RC_FILE_HANDLE_NOT_INIT;
        return msg;
    }
    // if(pageNum > fHandle->totalNumPages || pageNum < 0){
    //     msg = RC_READ_NON_EXISTING_PAGE;
    //     return msg;  
    // }
    FILE *fp = NULL;
    
    fp = fopen(fHandle->fileName, "r");
    
    if(fp == NULL){
        msg = RC_FILE_NOT_FOUND;
        return msg;
    }
    else{
        if(fseek(fp, pageNum * PAGE_SIZE, SEEK_SET) != 0){
            msg = RC_READ_NON_EXISTING_PAGE;
            fclose(fp);
            return msg;
        }
        else{
            fread(memPage, sizeof(char), PAGE_SIZE, fp);
            msg = RC_OK;
            fclose(fp);
            return msg;
        }
    }
       fclose(fp);
       return msg;
}

extern int getBlockPos (SM_FileHandle *fHandle){
    SM_PageHandle *page;
    SM_FileHandle *fp;
    int blockPos;
    blockPos = fHandle->curPagePos;
    return blockPos;
}     

extern RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    RC msg;
    SM_PageHandle *page;
    SM_FileHandle *fp;
    msg = readBlock(0,fHandle, memPage);
    return msg;
}

extern RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    RC msg;
    SM_PageHandle *page;
    SM_FileHandle *fp;
    int previousBlock = fHandle->curPagePos - 1;
    
    msg = readBlock(previousBlock,fHandle, memPage);
    return msg;
}

extern RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    RC msg;
    SM_PageHandle *page;
    SM_FileHandle *fp;
    int curBlock = fHandle->curPagePos;
    msg = readBlock(curBlock, fHandle, memPage);
    return msg;

}

extern RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    RC msg;
    SM_PageHandle *page;
    SM_FileHandle *fp;
    int nextBlock = fHandle->curPagePos + 1;
    msg = readBlock(nextBlock,fHandle, memPage);
    return msg;
}

extern RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    RC msg;
    SM_FileHandle *fp;
    SM_PageHandle *page;
    int lastBlock = fHandle->totalNumPages - 1;
    msg = readBlock(lastBlock, fHandle, memPage);
    return msg;

}

extern RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    RC msg;
    FILE *fp;
    fp = fopen(fHandle->fileName, "r+");
    
    if(fHandle==NULL){
        msg = RC_FILE_HANDLE_NOT_INIT;
        return msg;
    }
    
    // if(pageNum >= fHandle->totalNumPages || pageNum < 0){
    //     msg = RC_READ_NON_EXISTING_PAGE;
    //     return msg;
    // }
    if(fp==NULL){
        msg = RC_FILE_NOT_FOUND;
        return msg;
    }
    else{
        if(pageNum == 0){
            fseek(fp, pageNum*PAGE_SIZE ,SEEK_SET);
            int endOfFile;
            for(int i=0; i<4096; i++){
                endOfFile = feof(fp);
                if(endOfFile > 0){
                    appendEmptyBlock(fHandle);
                }
                fputc(memPage[i], fp);
            }
            fHandle->curPagePos = ftell(fp);
            int closeFile = fclose(fp);
            if(closeFile != 0){
                msg = RC_WRITE_FAILED;
            }
            else{
                msg = RC_OK;
            }
        }  
        else{
            fHandle->curPagePos = pageNum * PAGE_SIZE;
            fclose(fp);
            writeCurrentBlock(fHandle, memPage);
            msg = RC_OK;
        }
    }
    fclose(fp);
    return msg;     
}

extern RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    RC msg;
    SM_PageHandle *page;
    SM_FileHandle * fp;
    int curBlock = fHandle->curPagePos;
    return writeBlock(curBlock, fHandle, memPage);
}

extern RC appendEmptyBlock(SM_FileHandle *fHandle){
    RC msg;
    FILE *fp;
    fp = fopen(fHandle->fileName, "r+");
    SM_PageHandle emptyBlock = (SM_PageHandle) malloc(PAGE_SIZE * sizeof(char));
    if(fseek(fp,0,SEEK_END)==0){
        msg = RC_WRITE_FAILED;
        free(emptyBlock);
        return msg;
    }
    else{
        fwrite(emptyBlock,sizeof(char),PAGE_SIZE,fp);
        msg = RC_OK;
        free(emptyBlock);
        fHandle->totalNumPages = fHandle->totalNumPages + 1;
        return msg;
    }
    
free(emptyBlock);
fclose(fp);
return msg;

}

extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    RC msg;
    if(fHandle->totalNumPages < numberOfPages){
        int emptyPages = numberOfPages - fHandle->totalNumPages;  
        SM_PageHandle *ep = (SM_PageHandle*) malloc(emptyPages * PAGE_SIZE * sizeof(char));
        msg = RC_OK;
        free(ep);
        return msg;
    }
    return RC_OK;
}