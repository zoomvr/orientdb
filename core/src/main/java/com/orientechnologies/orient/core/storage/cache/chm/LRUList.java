package com.orientechnologies.orient.core.storage.cache.chm;

import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;

import java.util.Iterator;
import java.util.NoSuchElementException;

public final class LRUList implements Iterable<OCacheEntryImpl> {
  private int size;

  private OCacheEntryImpl head;
  private OCacheEntryImpl tail;

  void remove(final OCacheEntryImpl entry) {
    final OCacheEntryImpl next = entry.getNext();
    final OCacheEntryImpl prev = entry.getPrev();

    if (!(next != null || prev != null || entry == head)) {
      return;
    }

    assert prev == null || prev.getNext() == entry;
    assert next == null || next.getPrev() == entry;

    if (next != null) {
      next.setPrev(prev);
    }

    if (prev != null) {
      prev.setNext(next);
    }

    if (head == entry) {
      assert entry.getPrev() == null;
      head = next;
    }

    if (tail == entry) {
      assert entry.getNext() == null;
      tail = prev;
    }

    entry.setNext(null);
    entry.setPrev(null);
    entry.setContainer(null);

    size--;
  }

  boolean contains(final OCacheEntryImpl entry) {
    return entry.getContainer() == this;
  }

  void moveToTheTail(final OCacheEntryImpl entry) {
    if (tail == entry) {
      assert entry.getNext() == null;
      return;
    }

    final OCacheEntryImpl next = entry.getNext();
    final OCacheEntryImpl prev = entry.getPrev();

    final boolean newEntry = entry.getContainer() == null;
    assert entry.getContainer() == null || entry.getContainer() == this;

    assert prev == null || prev.getNext() == entry;
    assert next == null || next.getPrev() == entry;

    if (prev != null) {
      prev.setNext(next);
    }

    if (next != null) {
      next.setPrev(prev);
    }

    if (head == entry) {
      assert entry.getPrev() == null;
      head = next;
    }

    entry.setPrev(tail);
    entry.setNext(null);

    if (tail != null) {
      assert tail.getNext() == null;
      tail.setNext(entry);
      tail = entry;
    } else {
      tail = head = entry;
    }

    if (newEntry) {
      entry.setContainer(this);
      size++;
    } else {
      assert entry.getContainer() == this;
    }
  }

  int size() {
    return size;
  }

  OCacheEntryImpl poll() {
    if (head == null) {
      return null;
    }

    final OCacheEntryImpl entry = head;

    final OCacheEntryImpl next = head.getNext();
    assert next == null || next.getPrev() == head;

    head = next;
    if (next != null) {
      next.setPrev(null);
    }

    assert head == null || head.getPrev() == null;

    if (head == null) {
      tail = null;
    }

    entry.setNext(null);
    assert entry.getPrev() == null;

    size--;

    entry.setContainer(null);
    return entry;
  }

  OCacheEntryImpl peek() {
    return head;
  }

  public Iterator<OCacheEntryImpl> iterator() {
    return new Iterator<OCacheEntryImpl>() {
      private OCacheEntryImpl next = tail;

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public OCacheEntryImpl next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final OCacheEntryImpl result = next;
        next = next.getPrev();

        return result;
      }
    };
  }

}
