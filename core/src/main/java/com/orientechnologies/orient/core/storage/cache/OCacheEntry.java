package com.orientechnologies.orient.core.storage.cache;

import com.orientechnologies.orient.core.storage.cache.chm.LRUList;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tglman on 23/06/16.
 */
public final class OCacheEntry {
  private static final int FROZEN = -1;
  private static final int DEAD   = -2;

  private       OCachePointer dataPointer;
  private final long          fileId;
  private final long          pageIndex;

  private final AtomicInteger usagesCount = new AtomicInteger();
  private final AtomicInteger state       = new AtomicInteger();

  private OCacheEntry next;
  private OCacheEntry prev;

  private LRUList container;

  public OCacheEntry(final long fileId, final long pageIndex, final OCachePointer dataPointer) {
    this.fileId = fileId;
    this.pageIndex = pageIndex;

    this.dataPointer = dataPointer;
  }

  public OCachePointer getCachePointer() {
    return dataPointer;
  }

  public void clearCachePointer() {
    dataPointer = null;
  }

  public void setCachePointer(final OCachePointer cachePointer) {
    this.dataPointer = cachePointer;
  }

  public long getFileId() {
    return fileId;
  }

  public long getPageIndex() {
    return pageIndex;
  }

  public void acquireExclusiveLock() {
    dataPointer.acquireExclusiveLock();
  }

  public void releaseExclusiveLock() {
    dataPointer.releaseExclusiveLock();
  }

  public void acquireSharedLock() {
    dataPointer.acquireSharedLock();
  }

  public void releaseSharedLock() {
    dataPointer.releaseSharedLock();
  }

  public int getUsagesCount() {
    return usagesCount.get();
  }

  public void incrementUsages() {
    usagesCount.incrementAndGet();
  }

  /**
   * DEBUG only !!
   *
   * @return Whether lock acquired on current entry
   */
  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  public boolean isLockAcquiredByCurrentThread() {
    return dataPointer.isLockAcquiredByCurrentThread();
  }

  public void decrementUsages() {
    usagesCount.decrementAndGet();
  }

  public boolean acquireEntry() {
    int state = this.state.get();

    while (state >= 0) {
      if (this.state.compareAndSet(state, state + 1)) {
        return true;
      }

      state = this.state.get();
    }

    return false;
  }

  public void releaseEntry() {
    int state = this.state.get();

    while (true) {
      if (state <= 0) {
        throw new IllegalStateException("Cache entry " + fileId + ":" + pageIndex + " has invalid state " + state);
      }

      if (this.state.compareAndSet(state, state - 1)) {
        return;
      }

      state = this.state.get();
    }
  }

  public boolean isReleased() {
    return state.get() == 0;
  }

  public boolean isAlive() {
    return state.get() >= 0;
  }

  public boolean freeze() {
    int state = this.state.get();
    while (state == 0) {
      if (this.state.compareAndSet(state, FROZEN)) {
        return true;
      }

      state = this.state.get();
    }

    return false;
  }

  public boolean isFrozen() {
    return this.state.get() == FROZEN;
  }

  public void makeDead() {
    int state = this.state.get();

    while (state == FROZEN) {
      if (this.state.compareAndSet(state, DEAD)) {
        return;
      }

      state = this.state.get();
    }

    throw new IllegalStateException("Cache entry " + fileId + ":" + pageIndex + " has invalid state " + state);
  }

  public boolean isDead() {
    return this.state.get() == DEAD;
  }

  public OCacheEntry getNext() {
    return next;
  }

  public OCacheEntry getPrev() {
    return prev;
  }

  public void setPrev(final OCacheEntry prev) {
    this.prev = prev;
  }

  public void setNext(final OCacheEntry next) {
    this.next = next;
  }

  public void setContainer(final LRUList lruList) {
    this.container = lruList;
  }

  public LRUList getContainer() {
    return container;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final OCacheEntry that = (OCacheEntry) o;

    if (fileId != that.fileId)
      return false;
    return pageIndex == that.pageIndex;
  }

  @Override
  public int hashCode() {
    int result = (int) (fileId ^ (fileId >>> 32));
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "OCacheEntry{" + "dataPointer=" + dataPointer + ", fileId=" + fileId + ", pageIndex=" + pageIndex + ", usagesCount="
        + usagesCount + '}';
  }
}
