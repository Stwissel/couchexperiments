package com.notessensei.couchexperiments;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.logging.Logger;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonEventType;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.core.streams.WriteStream;

public class ChangelistenerWriteStream implements WriteStream<Buffer> {

  static final Logger LOGGER = Logger.getLogger(ChangelistenerWriteStream.class.getName());

  final Vertx vertx;
  final Consumer<JsonObject> consumer;
  final EntryPointProvider entryPointProvider;
  final JsonParser parser;
  Handler<Throwable> forExceptions;
  Handler<Void> forDrain;
  int writeQueueMaxSize = 1000;

  public ChangelistenerWriteStream(Vertx vertx, EntryPointProvider entryPointProvider,
      Consumer<JsonObject> consumer) {
    this.vertx = vertx;
    this.entryPointProvider = entryPointProvider;
    this.consumer = consumer;
    this.parser = JsonParser.newParser();
    setupParser(parser);
  }

  void setupParser(JsonParser p) {
    parser.objectValueMode();
    p.handler(event -> {
      if (event.type() == JsonEventType.VALUE) {
        JsonObject j = event.objectValue();
        Optional.ofNullable(j.getString("seq"))
            .ifPresent(seq -> {
              this.consumer.accept(j);
              entryPointProvider.setLastSequence(seq);
            });
      }

    });
    p.exceptionHandler(err -> {
      LOGGER.severe("Error parsing JSON: " + err.getMessage());
      forExceptions.handle(err);
    });

    p.endHandler(v -> {
      LOGGER.info("Parser ended");
      // forDrain.handle(null);
    });
  }

  @Override
  public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
    LOGGER.info("ChangelistenerWriteStream.exceptionHandler set");
    this.forExceptions = handler;
    return this;
  }

  @Override
  public Future<Void> write(Buffer data) {
    parser.handle(data);
    return Future.succeededFuture();
  }

  @Override
  public Future<Void> end() {
    LOGGER.info("ChangelistenerWriteStream.end called");
    parser.end();
    return this.entryPointProvider.saveLastSequence();
  }

  @Override
  public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
    LOGGER.info("ChangelistenerWriteStream.setWriteQueueMaxSize called with " + maxSize);
    this.writeQueueMaxSize = maxSize;
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    LOGGER.info("ChangelistenerWriteStream.writeQueueFull called");
    return false;
  }

  @Override
  public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
    LOGGER.info("ChangelistenerWriteStream.drainHandler called");
    this.forDrain = handler;
    return this;
  }

}
