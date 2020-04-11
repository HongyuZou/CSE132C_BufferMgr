/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "types.h"
#include "bufHashTbl.h"
#include "file.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
}

/**
* Advance clock to next frame in the buffer pool
*/
void BufMgr::advanceClock() {
	clockHand = (clockHand + 1) % numBufs;
}

/**
 * Allocate a free frame.  
 *
 * @param frame   	Frame reference, frame ID of allocated frame returned via this variable
 * @throws BufferExceededException If no such buffer is found which can be allocated
 */
void BufMgr::allocBuf(FrameId & frame) {
	// clock algorithm
	int numPinned = 0;
	
	while(true) {
		advanceClock();
		BufDesc buffer = this->bufDescTable[clockHand];
		
		// check if buffer all pinned
		if(numPinned == numBufs) {
			throw new BufferExceededException();
		}

		// check valid
		if(!buffer.valid) {
			frame = buffer.frameNo;
			break;
		}

		// check ref
		if(buffer.refbit) {
			buffer.refbit = false;
			continue;
		}

		// if pinned, continue
		if(buffer.pinCnt != 0) {
			numPinned ++;
			continue;
		}

		// is dirty
		if(buffer.dirty) {
			frame = buffer.frameNo;
			buffer.file->writePage(this->bufPool[frame]);
			break;
		}

		// delete hashtable entry
		this->hashTable->remove(buffer.file, buffer.pageNo);
		frame = buffer.frameNo;
		break;
	}
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
}

/**
 * Unpin a page from memory since it is no longer required for it to remain in memory.
 *
 * @param file   	File object
 * @param PageNo  Page number
 * @param dirty		True if the page to be unpinned needs to be marked dirty	
 * @throws  PageNotPinnedException If the page is not already pinned
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) {
	// check if frame in the table
	FrameId frame;
	try {
		this->hashTable->lookup(file, pageNo, frame);
	} catch(HashNotFoundException e) {
		return;
	}
	
	// check if pincnt == 0 and decrease pintCnt of frame
	int pinCnt = this->bufDescTable[frame].pinCnt;
	if(pinCnt == 0) {
		throw new PageNotPinnedException(file->filename, pageNo, frame);
	}
	this->bufDescTable[frame].pinCnt --;

	// check if it is dirty
	if(dirty) {
		this->bufDescTable[frame].dirty = 1;
	}
}

void BufMgr::flushFile(const File* file) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
}

/**
 * Delete page from file and also from buffer pool if present.
 * Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
 *
 * @param file   	File object
 * @param PageNo  Page number
 */
void BufMgr::disposePage(File* file, const PageId pageNo){
	// delete from buffer pool
	FrameId frame;
	try {
		this->hashTable->lookup(file, pageNo, frame);
		this->hashTable->remove(file, pageNo);
	} catch(HashNotFoundException e) {
	}

	// delete from file
	file->deletePage(pageNo);
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
