#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) {

    unsigned int count = 0;
    
    while (count < numBufs) {

        BufDesc* currDesc = &bufTable[clockHand];

        if (currDesc->pinCnt == 0) {
            //if pinCnt = 0 and refbit = true
            if (currDesc->refbit) {
                // make refbit to false (clock algorithm)
                currDesc->refbit = false;
            } 
            else {
                // if curr is valid and dirty => can allocate
                if (currDesc->valid && currDesc->dirty) {
// writePage call: pageNo, bufPool[clockHand] data address
                    Status writeStatus = currDesc->file->writePage(currDesc->pageNo, &bufPool[clockHand]);
                    // case 1: Not OK (something wrong)
                    if (writeStatus != OK) return writeStatus;
                    hashTable->remove(currDesc->file, currDesc->pageNo);
                    currDesc->dirty = false;
                }
                
                //case 2: OK (successfully allocated)
                frame = clockHand;
                return OK;
            }
        } else {
            count++;
        }

        
        // clockHand pointer change after allocation
        clockHand = (clockHand + 1) % numBufs;
    }
    // if all pinCnt>0, buffer exceeded
    return BUFFEREXCEEDED;
}

	
// find the page in buffer pool / read from disk 
// find the page in buffer pool / read from disk 
const Status BufMgr::readPage(File* file, const int pageNo, Page*& page) {

    int frameNo;
    // find the frame that we look for
    Status lookupStatus = hashTable->lookup(file, pageNo, frameNo);

    // case 1: if there is NO page we want to read in buffer pool
    if (lookupStatus == HASHNOTFOUND) {
        Status allocStatus = allocBuf(frameNo);

        if (allocStatus != OK) return allocStatus;
        Status readStatus = file->readPage(pageNo, &bufPool[frameNo]);

        if (readStatus != OK) return readStatus;
        bufTable[frameNo].Set(file, pageNo);

        hashTable->insert(file, pageNo, frameNo);
        page = &bufPool[frameNo];
        return OK;
    }
    // case 2: 
    BufDesc* desc = &bufTable[frameNo];
    desc->pinCnt++;
    desc->refbit = true;
    page = &bufPool[frameNo];
    return OK;
}



const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
// Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true, sets the dirty bit.  
// Returns OK if no errors occurred, HASHNOTFOUND if the page is not in the buffer pool hash table, PAGENOTPINNED if the pin count is already 0.
    int frameNo = 0;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    if(status == HASHNOTFOUND){
        return HASHNOTFOUND;
    }
    else {
        BufDesc* tmpBuf = (&bufTable[frameNo]);

        if(tmpBuf->pinCnt == 0){
            return PAGENOTPINNED;
        }
        if(dirty == true){
            tmpBuf->dirty = true;
        }
        tmpBuf->pinCnt--;
        return OK;

    }





}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
// This call is kind of weird.  The first step is to to allocate an empty page in the specified file by invoking the file->allocatePage() method. 
// This method will return the page number of the newly allocated page.  
// Then allocBuf() is called to obtain a buffer pool frame.  Next, an entry is inserted into the hash table and Set() is invoked on the frame to set it up properly.  
// The method returns both the page number of the newly allocated page to the caller via the pageNo 
// parameter and a pointer to the buffer frame allocated for the page via the page parameter. 
// Returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames are pinned and HASHTBLERROR if a hash table error occurred. 
    Status pageStatus = file->allocatePage(pageNo);
    if(pageStatus != OK){
        return pageStatus;
    }
    int frameNo = 0;
    Status status = allocBuf(frameNo);
    if(status != OK){
        return status;
    }
        BufDesc* tmpBuf = &bufTable[frameNo];
    tmpBuf->Set(file, pageNo);
    page = &bufPool[frameNo];
    if((hashTable->insert(file, pageNo, frameNo)) == HASHTBLERROR){
        return HASHTBLERROR;
    }
    return OK;

}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


