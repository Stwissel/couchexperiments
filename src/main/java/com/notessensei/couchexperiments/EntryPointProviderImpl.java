package com.notessensei.couchexperiments;

import java.util.Optional;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;

public class EntryPointProviderImpl implements EntryPointProvider {

  static final String FILENAME = "lastSequence.txt";

  String lastSequence = null;
  final Vertx vertx;

  public EntryPointProviderImpl(Vertx vertx) {
    this.vertx = vertx;
    loadLastSequence();
  }

  private void loadLastSequence() {
    // check for last sequence in a file
    // if not found, set to ""
    FileSystem fs = vertx.fileSystem();
    if (fs.existsBlocking(FILENAME)) {
      lastSequence = fs.readFileBlocking(FILENAME).toString();
    }
  }

  @Override
  public Optional<String> getLastSequence() {
    return Optional.ofNullable(lastSequence);
  }

  @Override
  public void setLastSequence(String sequence) {
    lastSequence = sequence;
  }

  @Override
  public Future<Void> saveLastSequence() {
    // Nothing to save is okay
    if (lastSequence == null) {
      return Future.succeededFuture();
    }
    Promise<Void> promise = Promise.promise();
    FileSystem fs = vertx.fileSystem();
    fs.writeFile(FILENAME, Buffer.buffer(lastSequence))
        .onSuccess(v -> promise.complete())
        .onFailure(promise::fail);
    return promise.future();
  }
}
