package com.notessensei.couchexperiments;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Logger;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpResponseExpectation;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;

public class ReceiverVerticle extends AbstractVerticle {

  static final Logger LOGGER = Logger.getLogger(ReceiverVerticle.class.getName());

  WebClient clientReceive;
  AtomicBoolean shutdownRequested = new AtomicBoolean(false);
  AtomicBoolean shutdownDone = new AtomicBoolean(false);

  final EntryPointProvider entryPointProvider;
  final Consumer<JsonObject> consumer;
  final String dbName;

  public ReceiverVerticle(EntryPointProvider entryPointProvider, Consumer<JsonObject> consumer,
      String dbName) {
    this.entryPointProvider = entryPointProvider;
    this.consumer = consumer;
    this.dbName = dbName;
  }

  Future<Void> startChangeListener() {
    LOGGER.info("startChangeListener called");

    final StringBuilder pathBuilder = new StringBuilder();
    pathBuilder.append("/")
        .append(this.dbName)
        .append("/_changes?feed=continuous&include_docs=true&timeout=4000");
    entryPointProvider.getLastSequence()
        .ifPresentOrElse(lastSequence -> {
          LOGGER.info("Last sequence: " + lastSequence);
          pathBuilder.append("&since=");
          pathBuilder.append(lastSequence);
        }, () -> LOGGER.info("No last sequence found"));

    ChangelistenerWriteStream writeStream =
        new ChangelistenerWriteStream(vertx, entryPointProvider, consumer);
    System.out.println(pathBuilder.toString());
    clientReceive
        .get(5984, "localhost", pathBuilder.toString())
        .authentication(CouchHelper.testCredentials())
        .as(BodyCodec.pipe(writeStream))
        .send()
        .expecting(HttpResponseExpectation.SC_SUCCESS)
        .onFailure(err -> {
          LOGGER.info("Error in change listener: " + err.getMessage());
          this.shutdownDone.set(true);
        })
        .onSuccess(ar -> {

          if (this.shutdownRequested.get()) {
            LOGGER.info("Change listener completed");
            this.shutdownDone.set(true);
          } else {
            LOGGER.info("Change listener restart");
            startChangeListener();
          }
        });
    return Future.succeededFuture();
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    LOGGER.info("ReceiverVerticle started");
    clientReceive = WebClient.create(vertx);
    this.getVertx().eventBus().consumer("shutdown", msg -> {
      LOGGER.info("Shutdown requested");
      shutdownRequested.set(true);
    });
    CouchHelper.createDbIfMissing(clientReceive, "_users")
        .compose(v -> CouchHelper.createDbIfMissing(clientReceive, "_replicator"))
        .compose(v -> CouchHelper.createDbIfMissing(clientReceive, "couchexperiments"))
        .compose(v -> startChangeListener())
        .onSuccess(v -> {
          LOGGER.info("Databases checked and/or created");
          startPromise.complete();
        })
        .onFailure(startPromise::fail);
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    this.shutdownRequested.set(true);
    this.getVertx().setPeriodic(1000, id -> {
      if (this.shutdownDone.get()) {
        this.getVertx().cancelTimer(id);
        LOGGER.info("ReceiverVerticle stopped");
        stopPromise.complete();
      }
    });
  }



}
