/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.storage.cache;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

import java.io.IOException;

/**
 * This class is heart of OrientDB storage model it presents disk backed data cache which works with direct memory.
 * <p>
 * Model of this cache is based on page model. All direct memory area is mapped to disk files and each file is split on pages. Page
 * is smallest unit of work. The amount of RAM which can be used for data manipulation is limited so only a subset of data will be
 * really loaded into RAM on demand, if there is not enough RAM to store all data, part of them will by flushed to the disk. If disk
 * cache is closed all changes will be flushed to the disk.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 14.03.13
 */
public interface OReadCache {
  long addFile(String fileName, OWriteCache writeCache) throws IOException;

  long addFile(String fileName, long fileId, OWriteCache writeCache) throws IOException;

  OCacheEntryImpl loadForWrite(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount,
      boolean verifyChecksums, OLogSequenceNumber startLSN) throws IOException;

  OCacheEntryImpl loadForRead(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount,
      boolean verifyChecksums) throws IOException;

  void releaseFromRead(OCacheEntryImpl cacheEntry, OWriteCache writeCache);

  void releaseFromWrite(OCacheEntryImpl cacheEntry, OWriteCache writeCache);

  void pinPage(OCacheEntryImpl cacheEntry, OWriteCache writeCache);

  OCacheEntryImpl allocateNewPage(long fileId, OWriteCache writeCache, OLogSequenceNumber startLSN)
      throws IOException;

  long getUsedMemory();

  void clear();

  void truncateFile(long fileId, OWriteCache writeCache) throws IOException;

  void closeFile(long fileId, boolean flush, OWriteCache writeCache);

  void deleteFile(long fileId, OWriteCache writeCache) throws IOException;

  void deleteStorage(OWriteCache writeCache) throws IOException;

  /**
   * Closes all files inside of write cache and flushes all associated data.
   *
   * @param writeCache Write cache to close.
   */
  void closeStorage(OWriteCache writeCache) throws IOException;

  /**
   * Load state of cache from file system if possible.
   *
   * @param writeCache Write cache is used to load pages back into cache if possible.
   */
  void loadCacheState(OWriteCache writeCache);

  /**
   * Stores state of cache inside file if possible.
   *
   * @param writeCache Write cache which manages files cache state of which is going to be stored.
   */
  void storeCacheState(OWriteCache writeCache);

  void changeMaximumAmountOfMemory(long calculateReadCacheMaxMemory);
}
