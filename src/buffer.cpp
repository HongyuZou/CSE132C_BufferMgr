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
	// flushes dirty pages
	for(int i = 0; i < numBufs; i ++) {
		BufDesc bufDes = this->bufDescTable[i];

		if(bufDes.dirty) {
			bufDes.file->writePage(this->bufPool[bufDes.frameNo]);
		}
	}

	// delete tables
	delete[] bufPool;
	delete[] bufDescTable;
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
			throw BufferExceededException();
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

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page) {
	// look up the page
	FrameId frame;
	try {
		this->hashTable->lookup(file, pageNo, frame);
	} catch (HashNotFoundException& e) {
		// allocate frame
		this->allocBuf(frame);
		
		// read page
		Page new_page = file->readPage(pageNo);
		this->bufPool[frame] = new_page;

		// insert page into hashtable
		this->hashTable->insert(file, pageNo, frame);
		
		// set
		this->bufDescTable[frame].Set(file, pageNo);

		// return
		page = &new_page;
		return;
	}

	// set refbit
	this->bufDescTable[frame].pinCnt++;
	this->bufDescTable[frame].refbit = true;
	page = &bufPool[frame];
	return;
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
	} catch(HashNotFoundException& e) {
		return;
	}
	
	// check if pincnt == 0 and decrease pintCnt of frame
	int pinCnt = this->bufDescTable[frame].pinCnt;
	if(pinCnt == 0) {
		throw PageNotPinnedException(file->filename(), pageNo, frame);
	}
	this->bufDescTable[frame].pinCnt --;

	// check if it is dirty
	if(dirty) {
		this->bufDescTable[frame].dirty = 1;
	}
}

/**
 * Writes out all dirty pages of the file to disk.
 * All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
 * Otherwise Error returned.
 *
 * @param file   	File object
 * @throws  PagePinnedException If any page of the file is pinned in the buffer pool 
 * @throws BadBufferException If any frame allocated to the file is found to be invalid
 */
void BufMgr::flushFile(const File* file) {
	// scan buf table
	for(int i = 0; i < numBufs; i ++) {
		BufDesc bufDes = this->bufDescTable[i];
		File* buf_file = bufDes.file;

		// check if belong to the file
		if(buf_file->filename() == file->filename()) {
			// check if valid
			if(!bufDes.valid) {
				throw BadBufferException(bufDes.frameNo, bufDes.dirty, 
				                         bufDes.valid, bufDes.refbit);
			}

			// if pinned
			if(bufDes.pinCnt > 0){
				throw PagePinnedException(buf_file->filename(), 
				                          bufDes.pageNo, bufDes.frameNo);
			}

			// dirty -> flush and set dirty to false
			if(bufDes.dirty) {
				bufDes.file->writePage(this->bufPool[bufDes.frameNo]);
				this->bufDescTable[i].dirty = false;
			}

			// remove from hashtable
			this->hashTable->remove(file, bufDes.pageNo);

			//invoke clear for the page
			this->bufDescTable[i].Clear();
		}
	}
}

/**
 * Allocates a new, empty page in the file and returns the Page object.
 * The newly allocated page is also assigned a frame in the buffer pool.
 *
 * @param file   	File object
 * @param PageNo  Page number. The number assigned to the page in the file is returned via this reference.
 * @param page  	Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) {
	// allocate empty page
	Page new_page = file->allocatePage();

	// alloc buf
	FrameId frame;
	this->allocBuf(frame);

	// insert to hashtable
	this->hashTable->insert(file, new_page.page_number(), frame);
	
	// set page
	this->bufDescTable[frame].Set(file, new_page.page_number());

	// set return value
	pageNo = new_page.page_number();
	this->bufPool[frame] = new_page;
	page = &this->bufPool[frame];
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
	} catch(HashNotFoundException& e) {
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
