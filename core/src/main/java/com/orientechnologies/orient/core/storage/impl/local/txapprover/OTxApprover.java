package com.orientechnologies.orient.core.storage.impl.local.txapprover;

/**
 * Interface which is used to test correctness of transactions.
 * Typically this interface does nothing. But if you need to rollback transaction you need to throw runtime exception.
 */
public interface OTxApprover {
  void approveTx();
}
