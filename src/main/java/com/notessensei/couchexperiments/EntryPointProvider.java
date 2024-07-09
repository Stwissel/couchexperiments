package com.notessensei.couchexperiments;

import java.util.Optional;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Get and set the last known sequence number for a CouchDB database.
 */
public interface EntryPointProvider {

  Optional<String> getLastSequence();

  void setLastSequence(String sequence);

  /**
   * Save the last sequence number to a file.
   */
  Future<Void> saveLastSequence();

  static EntryPointProvider create(Vertx vertx) {
    return new EntryPointProviderImpl(vertx);
  }

}
