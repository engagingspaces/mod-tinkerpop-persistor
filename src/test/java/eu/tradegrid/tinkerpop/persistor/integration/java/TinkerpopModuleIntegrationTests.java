/*
 * Copyright (c) 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.tradegrid.tinkerpop.persistor.integration.java;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.assertNotNull;
import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.fail;
import static org.vertx.testtools.VertxAssert.testComplete;

import io.vertx.rxcore.java.eventbus.RxEventBus;
import io.vertx.rxcore.java.eventbus.RxMessage;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import rx.Observable;
import rx.util.functions.Action1;
import rx.util.functions.Func1;

/**
 * Tinkerpop Persistor Bus Module integration tests for 
 * <a href="http://www.neo4j.org/">Neo4J</a> and
 * <a href="https://github.com/orientechnologies/orientdb">OrientDB</a>.
 * <p/>
 * @author <a href="https://github.com/aschrijver">Arnold Schrijver</a>
 */
public class TinkerpopModuleIntegrationTests extends TestVerticle {
    
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private JsonObject config;
    private RxEventBus rxEventBus;
    
    Object vertexId;
    Object outVertex;
    
    @Override
    public void start() {
        initialize();

        rxEventBus = new RxEventBus(vertx.eventBus());
        
        try {
            tempFolder.create();
        } catch (IOException e) {
            fail("Cannot open temporary folder for graph database.");
        }
        
        //config = getNeo4jConfig();
        config = getOrientDbConfig();
        
        container.deployModule(System.getProperty("vertx.modulename"), config,
                new AsyncResultHandler<String>() {
            
            @Override
            public void handle(AsyncResult<String> asyncResult) {
                assertTrue(asyncResult.succeeded());
                assertNotNull("deploymentID should not be null", asyncResult.result());

                startTests();
            }
        });
    }
    
    @Test
    public void testAddGetAndRemoveVertex() {

        // Load sample GraphSON message.
        JsonObject vertexToAdd = getResourceAsJson("addVertex.json");
        
        final Action1<Exception> failure = new Action1<Exception>() {

            @Override
            public void call(Exception e) {
                fail(e.getMessage());
            }
        };
        
        final Action1<RxMessage<JsonObject>> testComplete = new Action1<RxMessage<JsonObject>>() {

            @Override
            public void call(RxMessage<JsonObject> message) {
                assertEquals("ok", message.body().getString("status"));
                assertEquals(vertexId, message.body().getField("_id"));
                testComplete();
            }
        };
        
        final Func1<RxMessage<JsonObject>, Observable<RxMessage<JsonObject>>> removeVertex = 
                new Func1<RxMessage<JsonObject>, Observable<RxMessage<JsonObject>>>() {

            @Override
            public Observable<RxMessage<JsonObject>> call(RxMessage<JsonObject> message) {
                assertEquals("ok", message.body().getString("status"));
                assertNotNull("GraphSON: 'graph' object missing", message.body().getObject("graph"));
                assertNotNull("GraphSON: 'vertices' array missing", message.body().getObject("graph").getArray("vertices"));
                assertEquals(1, message.body().getObject("graph").getArray("vertices").size());
                assertTrue(message.body().getObject("graph").getArray("vertices").get(0) instanceof JsonObject);
                
                JsonObject vertex = message.body().getObject("graph").getArray("vertices").get(0);
                assertEquals("vert.x", vertex.getString("project"));
                assertNotNull(vertex.getField("_id"));
                vertexId = vertex.getField("_id");
                
                JsonObject vertexToRemove = new JsonObject();
                vertexToRemove.putString("action", "removeVertex");
                vertexToRemove.putValue("_id", vertexId);
                
                return rxEventBus.send("test.persistor", vertexToRemove);
            }
        };
        
        final Func1<RxMessage<JsonObject>, Observable<RxMessage<JsonObject>>> getVertex = 
                new Func1<RxMessage<JsonObject>, Observable<RxMessage<JsonObject>>>() {

            @Override
            public Observable<RxMessage<JsonObject>> call(RxMessage<JsonObject> message) {
                assertEquals("ok", message.body().getString("status"));
                assertNotNull(message.body().getValue("_id"));
                
                Object addedVertexId = message.body().getField("_id");
                
                // NOTE: The id returned depends on the graphdb implementation used and is not
                //       guaranteed to be equal to the one provided in the GraphSON message.
                assertNotNull("AddVertex must return a Vertex Id", addedVertexId);
                
                JsonObject vertexToGet = new JsonObject();
                vertexToGet.putString("action", "getVertex");
                vertexToGet.putValue("_id", addedVertexId);
                
                return rxEventBus.send("test.persistor", vertexToGet);
            }
        };
        
        rxEventBus.send("test.persistor", vertexToAdd)
                .mapMany(getVertex)
                .mapMany(removeVertex)
                .subscribe(testComplete, failure);   
    }
    
    @Test
    public void testAddVertexAndEdge() {
        
        // Load sample GraphSON message derived from Neo4J documentation.
        JsonObject graphToAdd = getResourceAsJson("neo4jAclGraphExample.json");
        JsonObject message = new JsonObject().putString("action", "addGraph")
                .putObject("graph", graphToAdd);
        
        vertx.eventBus().send("test.persistor", message, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(Message<JsonObject> message) {
                JsonObject reply = message.body();
                assertEquals("ok", reply.getString("status"));
                
                final JsonObject getVertex = new JsonObject()
                        .putString("action", "getVertices")
                        .putString("key", "name")
                        .putString("value", "User1 Home");
        
                vertx.eventBus().send("test.persistor", getVertex, new Handler<Message<JsonObject>>() {
        
                    @Override
                    public void handle(Message<JsonObject> message) {
                        JsonObject reply = message.body();
                        assertEquals("ok", reply.getString("status"));
                        assertNotNull(((JsonObject) message.body().getObject("graph")
                                .getArray("vertices").get(0)).getField("_id"));

                        outVertex = ((JsonObject) message.body().getObject("graph")
                                .getArray("vertices").get(0)).getField("_id");
                        
                        final JsonObject addVertex = new JsonObject()
                                .putString("action", "addVertex")
                                .putArray("vertices", new JsonArray()
                                        .addObject(new JsonObject()
                                                .putString("name", "My File2.pdf")));
                        
                        vertx.eventBus().send("test.persistor", addVertex, new Handler<Message<JsonObject>>() {
                            
                            @Override
                            public void handle(Message<JsonObject> message) {
                                JsonObject reply = message.body();
                                assertEquals("ok", reply.getString("status"));
                                
                                Object inVertex = reply.getValue("_id");
                                assertNotNull(inVertex);
                                
                                final JsonObject addEdge = new JsonObject()
                                        .putString("action", "addEdge")
                                        .putArray("edges", new JsonArray()
                                                .addObject(new JsonObject()
                                                        .putValue("_inV", inVertex)
                                                        .putValue("_outV", outVertex)
                                                        .putString("_label", "HAS_CHILD_CONTENT")));
                                
                                vertx.eventBus().send("test.persistor", addEdge, new Handler<Message<JsonObject>>() {
                                    
                                    @Override
                                    public void handle(Message<JsonObject> message) {
                                        JsonObject reply = message.body();
                                        assertEquals("ok", reply.getString("status"));
                                        assertNotNull(reply.getField("_id"));
                                        
                                        testComplete();
                                    }
                                });
                            }
                        });
                    }
                });
            }
        });
    }
    
    @Test
    public void testAddGraph() {
        
        // Load sample GraphSON message derived from Neo4J documentation.
        JsonObject graphToAdd = getResourceAsJson("neo4jAclGraphExample.json");
        JsonObject message = new JsonObject().putString("action", "addGraph")
                .putObject("graph", graphToAdd);
        
        vertx.eventBus().send("test.persistor", message, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(Message<JsonObject> message) {
                JsonObject reply = message.body();
                
                assertEquals("ok", reply.getString("status"));
                
                // Graph is only returned if ID's were generated by the db
                if (reply.getObject("graph") != null) {
                    assertNotNull("GraphSON: 'vertices' array missing", message.body().getObject("graph").getArray("vertices"));
                    assertNotNull("GraphSON: 'edges' array missing", message.body().getObject("graph").getArray("edges"));
                    assertEquals(19, message.body().getObject("graph").getArray("edges").size());
                    
                    JsonArray vertices = message.body().getObject("graph").getArray("vertices");
                    if (config.getObject("tinkerpopConfig").getString("blueprints.neo4j.directory") != null) {

                        // In Neo4J we have to account for auto-generated root vertex.
                        assertEquals(13, vertices.size());
                    } else {
                        assertEquals(12, vertices.size());
                    }
                    
                    assertTrue(message.body().getObject("graph").getArray("vertices").get(0) instanceof JsonObject);
                    assertTrue(message.body().getObject("graph").getArray("edges").get(0) instanceof JsonObject);
                }

                testComplete();
            }            
        });
    }
    
    @Test
    public void testQueryGraph() {
        final String query = "_().in('HAS_CHILD_CONTENT').loop(1){it.loops < 3}{it.object.name == 'Root folder'}.path";
        
        // Load sample GraphSON message derived from Neo4J documentation.
        JsonObject graphToAdd = getResourceAsJson("neo4jAclGraphExample.json");
        JsonObject message = new JsonObject().putString("action", "addGraph")
                .putObject("graph", graphToAdd);
        
        vertx.eventBus().send("test.persistor", message, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(Message<JsonObject> message) {
                JsonObject reply = message.body();
                assertEquals("ok", reply.getString("status"));
                
                final JsonObject getStartVertex = new JsonObject()
                        .putString("action", "getVertices")
                        .putString("key", "name")
                        .putString("value", "User1 Home");
                
                vertx.eventBus().send("test.persistor", getStartVertex, new Handler<Message<JsonObject>>() {

                    @Override
                    public void handle(Message<JsonObject> message) {
                        JsonObject reply = message.body();
                        assertEquals("ok", reply.getString("status"));
                        assertNotNull("GraphSON: 'graph' object missing", message.body().getObject("graph"));
                        assertNotNull("GraphSON: 'vertices' array missing", message.body().getObject("graph").getArray("vertices"));
                        assertEquals(1, message.body().getObject("graph").getArray("vertices").size());
                        
                        final Object id = ((JsonObject) reply.getObject("graph").getArray("vertices").get(0)).getField("_id");
                        final JsonObject queryRootFolder = new JsonObject()
                                .putString("action", "query")
                                .putString("query", query)
                                .putValue("_id", id);
                        
                        vertx.eventBus().send("test.persistor", queryRootFolder, new Handler<Message<JsonObject>>() {

                            @Override
                            public void handle(Message<JsonObject> message) {
                                JsonObject reply = message.body();
                                assertEquals("ok", reply.getString("status"));
                                
                                assertNotNull(reply.getArray("results"));
                                assertEquals(3, reply.getArray("results").size());
                                
                                testComplete();
                            }
                        });
                    }
                });
            }
        });
    }

    @Test
    public void testAddGetAndDropKeyIndex() {

        // Load sample GraphSON message derived from Neo4J documentation.
        JsonObject graphToAdd = getResourceAsJson("neo4jAclGraphExample.json");
        JsonObject message = new JsonObject().putString("action", "addGraph")
                .putObject("graph", graphToAdd);
        
        vertx.eventBus().send("test.persistor", message, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(Message<JsonObject> message) {
                JsonObject reply = message.body();
                assertEquals("ok", reply.getString("status"));

                JsonObject createKeyIndex = new JsonObject()
                        .putString("action", "createKeyIndex")
                        .putString("key", "name")
                        .putString("elementClass", "Vertex");
                
                vertx.eventBus().send("test.persistor", createKeyIndex, new Handler<Message<JsonObject>>() {

                    @Override
                    public void handle(Message<JsonObject> message) {
                        JsonObject reply = message.body();
                        assertEquals("ok", reply.getString("status"));
                        
                        JsonObject getIndexedKeys = new JsonObject()
                                .putString("action", "getIndexedKeys")
                                .putString("elementClass", "Vertex");
                        
                        vertx.eventBus().send("test.persistor", getIndexedKeys, new Handler<Message<JsonObject>>() {

                            @Override
                            public void handle(Message<JsonObject> message) {
                                JsonObject reply = message.body();
                                assertEquals("ok", reply.getString("status"));
                                assertNotNull(reply.getArray("keys"));
                                assertEquals(1, reply.getArray("keys").size());
                                assertEquals("name", reply.getArray("keys").get(0));
                                
                                JsonObject dropKeyIndex = new JsonObject()
                                        .putString("action", "dropKeyIndex")
                                        .putString("key", "name")
                                        .putString("elementClass", "Vertex");

                                vertx.eventBus().send("test.persistor", dropKeyIndex, new Handler<Message<JsonObject>>() {

                                    @Override
                                    public void handle(Message<JsonObject> message) {
                                        JsonObject reply = message.body();
                                        assertEquals("ok", reply.getString("status"));
                                        testComplete();
                                    }
                                });
                            }                            
                        });
                    }
                });
            }
        });
    }
    
    private JsonObject getNeo4jConfig() {
        JsonObject neo4jConfig = new JsonObject();
        neo4jConfig.putString(
                "blueprints.graph", "com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph");
        neo4jConfig.putString("blueprints.neo4j.directory", tempFolder.getRoot().getPath());

        JsonObject config = new JsonObject();
        config.putString("address", "test.persistor");
        config.putObject("tinkerpopConfig", neo4jConfig);
        
        return config;
    }
    
    private JsonObject getOrientDbConfig() {
        JsonObject orientDbConfig = new JsonObject();
        orientDbConfig.putString(
                "blueprints.graph", "com.tinkerpop.blueprints.impls.orient.OrientGraph");
        orientDbConfig.putString("blueprints.orientdb.url", "plocal:" + tempFolder.getRoot().getPath());
        
        // TODO: Add your own user here in <orientdb-location>/config/orientdb-server-config.xml
        orientDbConfig.putString("blueprints.orientdb.username", "admin");
        orientDbConfig.putString("blueprints.orientdb.password", "admin");

        // New configuration options (available in v1.6.0-SNAPSHOT).
        orientDbConfig.putBoolean("blueprints.orientdb.saveOriginalIds", true);
        orientDbConfig.putBoolean("blueprints.orientdb.lightweightEdges", true);
        
        JsonObject config = new JsonObject();
        config.putString("address", "test.persistor");
        config.putObject("tinkerpopConfig", orientDbConfig);
        
        return config;        
    }
    
    private JsonObject getResourceAsJson(String filename) {
        InputStream is = ClassLoader.getSystemResourceAsStream(filename);
        String jsonData = null;
        try {
            jsonData = IOUtils.toString(is, "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new JsonObject(jsonData);
    }
}
