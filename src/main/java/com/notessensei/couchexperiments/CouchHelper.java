package com.notessensei.couchexperiments;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpResponseExpectation;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.authentication.UsernamePasswordCredentials;
import io.vertx.ext.web.client.WebClient;

public class CouchHelper {

  public static Credentials testCredentials() {
    return new UsernamePasswordCredentials("admin", "password");
  }

  public static Future<Void> createDbIfMissing(WebClient client, String dbName) {
    System.out.println("createDbIfMissing " + dbName + " called");
    Promise<Void> promise = Promise.promise();
    client.get(5984, "localhost", "/" + dbName)
        .authentication(testCredentials())
        .timeout(3000)
        .send()
        .expecting(HttpResponseExpectation.SC_SUCCESS)
        .expecting(HttpResponseExpectation.JSON)
        .onSuccess(v -> promise.complete())
        .onFailure(err -> createDb(client, dbName, promise));

    return promise.future();
  }

  static void createDb(WebClient client, String dbName, Promise<Void> promise) {
    System.out.println("createDb" + dbName + " called");
    client.put(5984, "localhost", "/" + dbName)
        .authentication(testCredentials())
        .timeout(3000)
        .send()
        .expecting(HttpResponseExpectation.SC_SUCCESS)
        .expecting(HttpResponseExpectation.JSON)
        .onSuccess(v -> promise.complete())
        .onFailure(promise::fail);
  }


  private CouchHelper() {
    // Utility class, static methods only
  }
}
