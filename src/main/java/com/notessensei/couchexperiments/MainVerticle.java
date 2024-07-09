package com.notessensei.couchexperiments;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Logger;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.UsernamePasswordCredentials;
import io.vertx.ext.web.client.WebClient;

public class MainVerticle extends AbstractVerticle {

  static final Logger LOGGER = Logger.getLogger(MainVerticle.class.getName());

  public static void main(String[] args) {
    VertxOptions options = new VertxOptions();
    options.setBlockedThreadCheckInterval(180);
    options.setBlockedThreadCheckIntervalUnit(java.util.concurrent.TimeUnit.SECONDS);
    Vertx vertx = Vertx.vertx(options);
    vertx.deployVerticle(new MainVerticle())
        .onSuccess(id -> System.out.println("MainVerticle deployed: " + id))
        .onFailure(err -> {
          err.printStackTrace();
          System.exit(1);
        });
  }

  WebClient clientSend;
  AtomicInteger counter = new AtomicInteger(0);
  AtomicInteger roundCounter = new AtomicInteger(0);
  int max = 5;
  int maxRounds = 5;

  Future<Void> launchDocWarmup() {
    System.out.println("launchDocWarmup " + roundCounter.get());
    clientSend = WebClient.create(vertx);
    vertx.setPeriodic(500, this::sendOneMessage);
    return Future.succeededFuture();
  }

  void sendOneMessage(Long periodicId) {
    int curCounter = counter.getAndIncrement();
    if (curCounter > max) {
      vertx.cancelTimer(periodicId);
      System.out.println("Timer " + curCounter + " is done");
      int curRound = roundCounter.incrementAndGet();
      if (curRound < maxRounds) {
        System.out.println("Starting round " + curRound);
        counter.set(0);
        vertx.setTimer(10000, id -> this.launchDocWarmup());
      } else {
        vertx.close();
      }
      return;
    }
    String id = UUID.randomUUID().toString();
    clientSend.post(5984, "localhost", "/couchexperiments")
        .authentication(new UsernamePasswordCredentials("admin", "password"))
        .sendJson(new JsonObject()
            .put("creation", new Date().toString())
            .put("someData", id)
            .put("counter", counter.incrementAndGet()))
        .onSuccess(response -> System.out.print(" "))
        .onFailure(err -> System.out.println("Error in creating document: " + err.getMessage()));
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    EntryPointProvider entryPointProvider = EntryPointProvider.create(vertx);

    Consumer<JsonObject> consumer = j -> {
      LOGGER.info("****************\n" + j.encodePrettily());
    };

    String dbName = "couchexperiments";

    launchDocWarmup();
    vertx.deployVerticle(new ReceiverVerticle(entryPointProvider, consumer, dbName))
        .compose(v -> launchDocWarmup())
        .onSuccess(startPromise::complete)
        .onFailure(startPromise::fail);
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    // Delay shudown
    vertx.setTimer(5000, id -> {
      LOGGER.info("Shutting down MainVerticle.");
      stopPromise.complete();
    });
  }


}
