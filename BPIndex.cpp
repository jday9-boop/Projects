#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG


namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------
int leafMiddle; // global viable
int nodeMiddle;

BTreeIndex::BTreeIndex(const std::string & relationName,
    std::string & outIndexName,
    BufMgr *bufMgrIn,
    const int attrByteOffset,
    const Datatype attrType)
{
  bufMgr = bufMgrIn;
  leafOccupancy = INTARRAYLEAFSIZE;
  nodeOccupancy = INTARRAYNONLEAFSIZE;
  std::ostringstream idxStr;
  idxStr << relationName << "." << attrByteOffset;
  outIndexName = idxStr.str();
  leafMiddle = leafOccupancy % 2 == 1? (leafOccupancy/2) + 1: leafOccupancy/2;
  nodeMiddle = nodeOccupancy % 2 == 1? (nodeOccupancy/2) + 1: nodeOccupancy/2;
  scanExecuting = false;

  try {
    file = new BlobFile(outIndexName, false);
  } catch(FileNotFoundException e) {
    file = new BlobFile(outIndexName, true);
    PageFile pfile = PageFile::open(relationName);
    Page *headerPage;
    Page *rootPage;
    bufMgr->allocPage(file, headerPageNum, headerPage);
    bufMgr->allocPage(file, rootPageNum, rootPage);
    IndexMetaInfo *meta = (IndexMetaInfo *)headerPage;
    relationName.copy(meta->relationName, 20, 0);
    meta->attrByteOffset = attrByteOffset;
    meta->attrType = attrType;
    meta->rootPageNo = rootPageNum;
    initRootPageNum = rootPageNum;
    LeafNodeInt *root = (LeafNodeInt *)rootPage;
    root->rightSibPageNo = -1;

    //unpin page
    bufMgr->unPinPage(file, headerPageNum, true);
    bufMgr->unPinPage(file, rootPageNum, true);
    FileScan fscan(relationName, bufMgrIn);
    RecordId scanRid;
    try {
      while (1) {
        fscan.scanNext(scanRid);
        std::string recordStr = fscan.getRecord();
        const char *record = recordStr.c_str();
        int key = *((int *)(record + attrByteOffset));
        insertEntry(&key, scanRid);
      }
    } catch(const EndOfFileException &e) {
     
    }

  }

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	if (scanExecuting) endScan();
	bufMgr->flushFile(file);
	delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
  RIDKeyPair<int> entry;
  entry.set(rid, *((int *)key));
  Page* rootPage;
  bufMgr->readPage(file, rootPageNum, rootPage);
  LeafNodeInt *root = (LeafNodeInt *)rootPage;
  // the newChild is the push up entry, and it is set to null now
  PageKeyPair<int> *newChild = nullptr;
  if (rootPageNum == initRootPageNum) {
    // only in this case, the root page is a leaf page
    insertRecursive((Page*)root, rootPageNum,entry, true, newChild);
  } else {
    // otherwise, we can assume that the root is a non leaf page
    insertRecursive((Page*)root, rootPageNum, entry, false, newChild);
  }
}

// helper function called by insertentry
void BTreeIndex::insertRecursive(Page* curr, PageId currId, RIDKeyPair<int> data, bool isLeaf, PageKeyPair<int> *&newChild)
{
  if(isLeaf) { //insert into a leaf node
    LeafNodeInt* leafNode = (LeafNodeInt*) curr;
    // check if the leaf node is not full
    if(leafNode->ridArray[leafOccupancy - 1].page_number == 0) {
      // if it's not full, add directly
      insertLeaf(leafNode, &data);
      // finish the insert process, unpin current page
      bufMgr->unPinPage(file, currId, true);
      // we've done the insert of newChild, set it to null
      newChild = nullptr;
     
    } else {
      //else, split the current leaf node
      splitLeafNode(leafNode, currId, &data, newChild);
      // finish the insert process, unpin current page
      bufMgr->unPinPage(file, currId, true);
    }
  } else { // insert into a non leaf node
    NonLeafNodeInt* nonleafNode = (NonLeafNodeInt*) curr;
    Page *nextPage;
    PageId nextPageNum;
    // find the correct next page to read
    int i = 0;
    while(i < nodeOccupancy && (nonleafNode->pageNoArray[i+1] != 0) && (nonleafNode->keyArray[i] < data.key)) i++;
    nextPageNum = nonleafNode->pageNoArray[i];
    bufMgr->readPage(file, nextPageNum, nextPage);
    // do the insert on the next page
    insertRecursive(nextPage, nextPageNum, data, nonleafNode->level == 1? true: false, newChild);

    if (newChild == nullptr) {
      // there is no split in the child, we're done with insert, do the unpin
      bufMgr->unPinPage(file, currId, true);
    } else {
       if (nonleafNode->pageNoArray[nodeOccupancy] == 0) {
        // if the current page is not full, insert the new child entry into current page
        insertNonLeaf(nonleafNode, newChild);
        // done with insert, set newchildrentry to null, unpin page
        newChild = nullptr;
        bufMgr->unPinPage(file, currId, true);
      } else {
        // current page is full, split current non leaf node
        splitNonLeafNode(nonleafNode,currId,  newChild);
        //done with insert, unpin page
        bufMgr->unPinPage(file, currId, true);
      }
    }

  }
}
// insert the data entry directly into the correct position of the non leaf node
void BTreeIndex::insertNonLeaf(NonLeafNodeInt *nonleaf, PageKeyPair<int> *data)
{
    int i = getNodeSize(nonleaf);
    while( i > 0 && (nonleaf->keyArray[i-1] > data->key)) {
      nonleaf->keyArray[i] = nonleaf->keyArray[i-1];
      nonleaf->pageNoArray[i+1] = nonleaf->pageNoArray[i];
      i--;
    }
    // find the corret position, insert the data entry
    nonleaf->keyArray[i] = data->key;
    nonleaf->pageNoArray[i+1] = data->pageNo;
}

// insert the data entry directly into the correct position of the leaf node
void BTreeIndex::insertLeaf(LeafNodeInt *leaf, RIDKeyPair<int> *data)
{
  // if there is no data in this leaf node
  if (leaf->ridArray[0].page_number == 0) {
    leaf->keyArray[0] = data->key;
    leaf->ridArray[0] = data->rid;  
  } else {
    int i = getLeafSize(leaf);
    while( i >= 0 && (leaf->keyArray[i] > data->key)) {
      leaf->keyArray[i+1] = leaf->keyArray[i];
      leaf->ridArray[i+1] = leaf->ridArray[i];
      i--;
    }
    // find the corret position, insert the entry
    leaf->keyArray[i+1] = data->key;
    leaf->ridArray[i+1] = data->rid;
  }
}

//the leaf node is full, split the current leaf node and copy half of the data into it
void BTreeIndex::splitLeafNode(LeafNodeInt* leafNode, PageId leafPageNum,RIDKeyPair<int> *data, PageKeyPair<int> *&newChild)
{
  PageId newPageNum;
  Page* newPage;
  bufMgr->allocPage(file, newPageNum, newPage);
  LeafNodeInt *newLeafNode = (LeafNodeInt *)newPage;
  // set and reset the data in leafnode and newleafnode
  for(int i = leafMiddle; i < leafOccupancy; i++) {
      newLeafNode->keyArray[i-leafMiddle] = leafNode->keyArray[i];
      newLeafNode->ridArray[i-leafMiddle] = leafNode->ridArray[i];
      leafNode->keyArray[i] = 0;
      leafNode->ridArray[i].page_number = 0;
      leafNode->ridArray[i].slot_number = 0;
      leafNode->ridArray[i].padding = 0;
  }
  // insert the data into the leafNode or newLeadNode by its valye
  if (data->key < newLeafNode->keyArray[0]) {
    insertLeaf(leafNode, data);
  } else {
    insertLeaf(newLeafNode, data);
  }
  // set up the sibling of the new and old leaf node
  newLeafNode->rightSibPageNo = leafNode->rightSibPageNo;
  leafNode->rightSibPageNo = newPageNum;
  bufMgr->unPinPage(file, newPageNum, true);
  // we now get a new page key pair that need to be inserted into the parent node
  newChild = new PageKeyPair<int>();
  newChild->set(newPageNum, newLeafNode->keyArray[0]);

  // if the current page is the root page, need to have a new root page and update the relevant data
  if (leafPageNum == rootPageNum) {
    PageId newRootPageNum;
    Page *newRoot;
    bufMgr->allocPage(file, newRootPageNum, newRoot);
    NonLeafNodeInt *newRootPage = (NonLeafNodeInt *)newRoot;
    newRootPage->level = initRootPageNum == rootPageNum? 1: 0;
    newRootPage->pageNoArray[0] = leafPageNum;
    newRootPage->pageNoArray[1] = newChild->pageNo;
    newRootPage->keyArray[0] = newChild->key;
    rootPageNum = newRootPageNum;
    bufMgr->unPinPage(file, newRootPageNum, true);
    updateMeta(newRootPageNum);
  }

}

//the non leaf node is full, split the current non leaf node and copy half of the data into it
void BTreeIndex::splitNonLeafNode(NonLeafNodeInt *nonleafNode, PageId nonleafPageNum, PageKeyPair<int> *&newChild)
{
  PageId newPageNum;
  Page* newPage;
  bufMgr->allocPage(file, newPageNum, newPage);
  NonLeafNodeInt *newnNonleafNode = (NonLeafNodeInt *)newPage;
  PageKeyPair<int> pushupEntry; // the page key pair that need to be pushed up to the parent node after insert
  pushupEntry.set(newPageNum, nonleafNode->keyArray[nodeMiddle]);
   // set and reset the data in nonleafNode and newnNonleafNode
  for(int i = nodeMiddle + 1; i < nodeOccupancy; i++) {
    newnNonleafNode->keyArray[i-nodeMiddle - 1] = nonleafNode->keyArray[i];
    newnNonleafNode->pageNoArray[i-nodeMiddle+1] = nonleafNode->pageNoArray[i+1];
    nonleafNode->pageNoArray[i+1] = (PageId) 0;
    nonleafNode->keyArray[i] = 0;
  } 
  newnNonleafNode->level = nonleafNode->level;
  // set the data in push up position to empty
  nonleafNode->keyArray[nodeMiddle] = 0;
  newnNonleafNode->pageNoArray[0] = nonleafNode->pageNoArray[nodeMiddle + 1];
  nonleafNode->pageNoArray[nodeMiddle + 1] = (PageId) 0;

  bufMgr->unPinPage(file, newPageNum, true);
  newnNonleafNode->level = nonleafNode->level;
  // insert the data into the nonleafNode or newnNonleafNode by its valye
  if (newChild->key < newnNonleafNode->keyArray[0]) {
    insertNonLeaf(nonleafNode, newChild);
  } else {
    insertNonLeaf(newnNonleafNode, newChild);
  }
  // we are done with the insert of newChild, so set it to null
  newChild = nullptr;

  // if the current page is the root page, need to set a new root page and update the relevant data
  if (nonleafPageNum == rootPageNum) {
    PageId newRootPageNum;
    Page *newRoot;
    bufMgr->allocPage(file, newRootPageNum, newRoot);
    NonLeafNodeInt *newRootPage = (NonLeafNodeInt *)newRoot;
    newRootPage->level = initRootPageNum == rootPageNum? 1: 0;
    newRootPage->pageNoArray[0] =nonleafPageNum;
    newRootPage->pageNoArray[1] = pushupEntry.pageNo;
    newRootPage->keyArray[0] = pushupEntry.key;
    rootPageNum = newRootPageNum;
    bufMgr->unPinPage(file, newRootPageNum, true);
    updateMeta(newRootPageNum);
  }
}

// update the tree meta info with new root page id
void BTreeIndex::updateMeta(PageId newRootPageNum)
{
  Page *meta;
  bufMgr->readPage(file, headerPageNum, meta);
  IndexMetaInfo *metaPage = (IndexMetaInfo *)meta;
  metaPage->rootPageNo = newRootPageNum;
  bufMgr->unPinPage(file, headerPageNum, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
           const Operator lowOpParm,
           const void* highValParm,
           const Operator highOpParm)
{
 
  lowValInt = *((int *)lowValParm);
  highValInt = *((int *)highValParm);

  //check for valid operators and parameters
  if(!checkOps(lowOpParm, highOpParm))
  {
    throw BadOpcodesException();
  }
  if(lowValInt >= highValInt)
  {
    throw BadScanrangeException();
  }

  lowOp = lowOpParm;
  highOp = highOpParm;

  //check if scan is already executing
  if(scanExecuting)
  {
    endScan();
  }

  scanExecuting = true;

  currentPageNum = rootPageNum;
  // Start scanning by reading rootpage into the buffer pool
  bufMgr->readPage(file, currentPageNum, currentPageData);
  NonLeafNodeInt *current = (NonLeafNodeInt*) currentPageData;

  //while loop that iterates through non-leaf nodes until we reach a node that has level of 1, right above leaf nodes
  while (current->level == 0) {
    int i = 0;
    while (lowValInt > current->keyArray[i]) {
        i++;
        if (i == nodeOccupancy) {
          bufMgr->unPinPage(file, currentPageNum, false);
          currentPageNum = current->pageNoArray[i+1];
          bufMgr->readPage(file, currentPageNum, currentPageData);
          i = 0;
        }
    }
    current = (NonLeafNodeInt*) currentPageData;
  }
 
  //iterate through level 1 nodes until and stop until conditions are met
  int i = 0;
  while (lowValInt > current->keyArray[i]) {
      i++;
      if (i == nodeOccupancy) {
        bufMgr->unPinPage(file, currentPageNum, false);
        currentPageNum = current->pageNoArray[i+1];
        bufMgr->readPage(file, currentPageNum, currentPageData);
        i = 0;
      }
  }
  bufMgr->unPinPage(file, currentPageNum, false);
  //if current pages child doesn't exist, get right child instead
  if (current->pageNoArray[i] == 0) {
      currentPageNum = current->pageNoArray[i+1];
  }
  else {
      currentPageNum = current->pageNoArray[i];
  }
  bufMgr->readPage(file, currentPageNum, currentPageData);
  LeafNodeInt *leaf = (LeafNodeInt*) currentPageData;
  int index = 0;
  //iterate through leaf node until we are at the start of the scan parameter
  while (index < getLeafSize(leaf))
  {
    if (checkLow(lowValInt, lowOp, leaf->keyArray[index])) {
      nextEntry = index;
      return;
    }
    index++;
  }
  //if we reach this point without returning, we never identified the low end of the range, so we throw a NoSuchKeyFoundException()
  bufMgr->unPinPage(file, currentPageNum, false);
  throw NoSuchKeyFoundException();
}

/**
  * Fetch the record id of the next index entry that matches the scan.
  * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
  * @param outRid RecordId of next record found that satisfies the scan criteria returned in this
  * @throws ScanNotInitializedException If no scan has been initialized.
  * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
**/
const void BTreeIndex::scanNext(RecordId& outRid)
{
    if (scanExecuting != true) {
        throw ScanNotInitializedException();
    }

    LeafNodeInt* this_node = (LeafNodeInt*) currentPageData;
   
    //handles the case where scanNext is called but we are at the end of the current leaf node
    if (this_node->ridArray[nextEntry].page_number == 0 || nextEntry == leafOccupancy) {
        bufMgr->unPinPage(file, currentPageNum, false);
        if (this_node->rightSibPageNo == 0) {
            throw IndexScanCompletedException();
        }
        currentPageNum = this_node->rightSibPageNo;
        bufMgr->readPage(file, currentPageNum, currentPageData);
        this_node = (LeafNodeInt*) currentPageData;
        nextEntry = 0;
    }

    //if we haven't reached the end of the array, we skip moving to the next leaf, check the conditions on the key, and return the data.
    int this_key = this_node->keyArray[nextEntry];
    if (checkHigh(highValInt, highOp, this_key)) {
        outRid = this_node->ridArray[nextEntry];
        nextEntry++;
    }
    //new key didn't pass conditions, so we must have reached the end of the scan.
    else {
        throw IndexScanCompletedException();
    }
}
//helper method to compare key to the low end of the range
const bool BTreeIndex::checkLow(int lowVal, const Operator lowOp, int key)
{
    if (lowOp == GTE) {
        if (key >= lowVal) {
            return true;
        }
        else {
            return false;
        }
    }
    else if (lowOp == GT) {
        if (key > lowVal) {
            return true;
        }
        else {
            return false;
        }
    }
    else {
        return false;
    }
}

//helper method to compare key to the high end of the range
const bool BTreeIndex::checkHigh(int highVal, const Operator highOp, int key)
{
    if (highOp == LTE) {
        if (key <= highVal) {
            return true;
        }
        else {
            return false;
        }
    }
    else if (highOp == LT) {
        if (key < highVal) {
            return true;
        }
        else {
            return false;
        }
    }
    else {
        return false;
    }
}
//helper method which checks operators passed into scan
const bool BTreeIndex::checkOps(const Operator lowOp, const Operator highOp)
{
  if ((lowOp == GT || lowOp == GTE) && (highOp == LT || highOp == LTE)) {
    return true;
  }
  else {
    return false;
  }
}

/**
  * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
  * @throws ScanNotInitializedException If no scan has been initialized.
**/
const void BTreeIndex::endScan()
{
  if(!scanExecuting)
  {
    throw ScanNotInitializedException();
  }
  bufMgr->unPinPage(file, currentPageNum, true);
  scanExecuting = false;
  // Unpin page
}

//returns the current number of elements in a leafNode
const int BTreeIndex::getLeafSize(LeafNodeInt *curNode) {
  int i = leafOccupancy - 1;
  while (i > 0 && curNode->ridArray[i].page_number == 0) {
    i--;
  }
  return i;
}

//returns the current number of elements in a non leaf node
const int BTreeIndex::getNodeSize(NonLeafNodeInt *curNode) {
  int i = nodeOccupancy - 1;
  while (i > 0 && curNode->pageNoArray[i] == 0) {
    i--;
  }
  return i;
}
}
