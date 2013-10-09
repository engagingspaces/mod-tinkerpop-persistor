/*
 * Copyright 2013 the original author or authors.
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

package eu.tradegrid.tinkerpop.persistor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.EncodeException;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.groovy.Gremlin;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.iterators.SingleIterator;

import eu.tradegrid.tinkerpop.persistor.util.JsonUtility;

/**
 * Tinkerpop Persistor Bus Module
 * <p/>
 * This module uses <a href="https://github.com/tinkerpop/blueprints">Tinkerpop Blueprints</a> and
 * <a href="https://github.com/tinkerpop/gremlin">Tinkerpop Gremlin</a> to retrieve and persist 
 * data in a supported graph database.
 * <p/>
 * Please see the README.md for more detailed information.
 * <p/> 
 * @author Arnold Schrijver
 */
public class TinkerpopPersistor extends BusModBase implements Handler<Message<JsonObject>> {

    protected String address;
    protected Configuration tinkerpopConfig;
    protected JsonUtility jsonUtility;
    
    protected ConcurrentHashMap<String, Pipe<Element, Object>> queryCache;
    
    /**
     * Start the Tinkerpop Persistor module.
     */
    @Override
    public void start() {
        super.start();
        
        address = getOptionalStringConfig("address", "tinkerpop.persistor");
        tinkerpopConfig = loadTinkerpopConfig();
        jsonUtility = new JsonUtility(tinkerpopConfig.getString("graphson.mode", "NORMAL"));
        
        queryCache = new ConcurrentHashMap<>();
        
        eb.registerHandler(address, this);
        
        logger.info("TinkerpopPersistor module started");
    }
    
    /**
     * Stop the Tinkerpop Persistor module.
     */
    @Override
    public void stop() {
        logger.info("TinkerpopPersistor module stopped");
    }

    /**
     * Handle incoming events based on the action specified in the {@link Message}.<p/>
     * 
     * NOTE: 'Node' can be used instead of 'Vertex' in action terminology to avoid 
     * confusion with Vert.x own terminology. And instead of 'Edge' one can use 'Relationship'.
     * 
     * @param message the incoming vertx event
     */
    @Override
    public void handle(Message<JsonObject> message) {
        String action = getMandatoryString("action", message);
        if (action == null) {
            sendError(message, "Action must be specified");
            return;
        }
        
        final Graph graph;
        try {
            graph = GraphFactory.open(tinkerpopConfig);
        } catch (RuntimeException e) {
            sendError(message, "Cannot open Graph using Tinkerpop configuration");
            return;
        }
        
        try {
            switch (action) {
                case "addGraph":
                    addGraph(message, graph);
                    break;
                case "addVertex":
                case "addNode":
                    addVertex(message, graph);
                    break;
                case "query":
                    query(message, graph);
                    break;
                case "getVertices":
                case "getNodes":
                    getVertices(message, graph);
                    break;
                case "getVertex":
                case "getNode":
                    getVertex(message, graph);
                    break;
                case "removeVertex":
                case "removeNode":
                    removeVertex(message, graph);
                    break;
                case "addEdge":
                case "addRelationship":
                    addEdge(message, graph);
                    break;
                case "getEdge":
                case "getRelationship":
                    getEdge(message, graph);
                    break;
                case "getEdges":
                case "getRelationships":
                    getEdges(message, graph);
                    break;
                case "removeEdge":
                case "removeRelationship":
                    removeEdge(message, graph);
                    break;
                case "createKeyIndex":
                    createKeyIndex(message, graph);
                    break;
                case "dropKeyIndex":
                    dropKeyIndex(message, graph);
                    break;
                case "getIndexedKeys":
                    getIndexedKeys(message, graph);
                    break;
                case "flushQueryCache":
                    flushQueryCache(message, graph);
                    break;
                default:
                    sendError(message, "Unsupported action " + action);
                    break;
            }
        } catch (RuntimeException e) {
            if (graph instanceof TransactionalGraph) {
                ((TransactionalGraph) graph).rollback();
            }
            
            sendError(message, 
                    String.format("Action '%s': %s", action, e.getMessage()), e);
        } catch (Exception e) {
            if (graph instanceof TransactionalGraph) {
                ((TransactionalGraph) graph).rollback();
            }
            
            throw e;
        } finally {
            graph.shutdown();
        }
    }
    
    /**
     * Add a complete {@link Graph} to the db that may consist of multiple vertices and
     * edges. The graph in the message body must follow the GraphSON format.</p>
     * 
     * @param message the message containing information on the full Graph to create
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    protected void addGraph(Message<JsonObject> message, Graph graph) {
        JsonObject graphJson = getMandatoryObject("graph", message);
        if (graphJson == null) {
            sendError(message, "Action 'addGraph': No graphSON data supplied.");
            return;
        }
        
        try {
            jsonUtility.deserializeGraph(graph, graphJson);
        } catch (UnsupportedEncodingException e) {
            sendError(message, "Action 'addGraph': The Graphson message is not UTF-8 encoded", e);
            return;
        } catch (IOException e) {
            sendError(message, "Action 'addGraph': The Graphson message is invalid", e);
            return;
        }
        
        if (graph instanceof TransactionalGraph) {
            ((TransactionalGraph) graph).commit();
        }

        // Need to return the resulting Graph, if Id's have been generated.
        if (graph.getFeatures().ignoresSuppliedIds) {
            JsonObject reply;
            try {
                reply = jsonUtility.serializeGraph(graph);
            } catch (IOException e) {
                sendError(message, "Action 'addGraph': Cannot convert Graph to JSON", e);
                return;
            }
            
            sendOK(message, reply);
        } else {
            sendOK(message);
        }
    }
    
    /**
     * Add a new {@link Vertex} to the db and return a reply  with the Id of the 
     * newly created vertex. The vertex in the message body must follow the GraphSON format.
     * Only the first Vertex in the GraphSON message is processed, other vertices and
     * edges are ignored.<p/>
     * 
     * Note that the id is not guaranteed to be similar to that received in the incoming,
     * {@link Message} since Id generation may be database-vendor-specific.<p/>
     * 
     * If an error occurs and the graph was transactional, then a rollback will occur.<p/>
     * 
     * @param message the message containing information on the new Vertex to create
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    protected void addVertex(Message<JsonObject> message, Graph graph) {
        JsonArray verticesJson = message.body().getArray("vertices");
        if (verticesJson == null || verticesJson.size() == 0) {
            sendError(message, "Action 'addVertex': No vertex data supplied.");
        }
        
        Vertex vertex;
        try {
            vertex = jsonUtility.deserializeVertex(graph, (JsonObject) verticesJson.get(0));
        } catch (IOException e) {
            sendError(message, "Action 'addVertex': The Graphson message is invalid", e);
            return;
        }

        if (graph instanceof TransactionalGraph) {
            ((TransactionalGraph) graph).commit();
        } else if (graph.getFeatures().ignoresSuppliedIds) {
            // Shutting down the graph should force Id generation.
            graph.shutdown();
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug("Added Vertex with Id: " + vertex.getId().toString());
        }

        JsonObject reply = new JsonObject().putValue("_id", vertex.getId());
        try {
            sendOK(message, reply);
        } catch (EncodeException e) {
            // Id is not a Json support datatype, serialize to string.
            reply.putValue("_id", vertex.getId().toString());
            sendOK(message, reply);
        }
    }
    
    /**
     * Execute a Gremlin query starting from the {@link Vertex} or {@link Edge} specified by Id in
     * the message body, and by using the query string specified in the 'query' field.<p/>
     * The query will first be compiled to a Gremlin {@link Pipe} which is then iterated and
     * returned as JSON in the message reply.
     * <p/>
     * Currently there is only support for queries that deal with either {@link Vertex} or {@link Edge}
     * for their starts (and ends) types. 
     * 
     * @param message the message containing information on the Gremlin query to execute
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    @SuppressWarnings("unchecked")
    protected void query(Message<JsonObject> message, Graph graph) {
        String starts = message.body().getString("starts", "Vertex");
        Object id = getMandatoryValue(message, "_id");
        if (id == null) {
            return;
        }
        
        String query = getMandatoryString("query", message);
        if (query == null) {
            sendError(message, "Action 'query': No query specified.");
            return;
        }
        
        Element element = null;
        if ("Vertex".equals(starts) == true) {
            element = graph.getVertex(id);
        } else if ("Edge".equals(starts)) {
            element = graph.getEdge(id);
        } else {
            sendError(message, "Action 'query': Unsupported starts property: " + starts);
            return;
        }

        if (element == null) {
            sendError(message, String.format("Action 'query': Starting %s %s not found", 
                    starts, id.toString()));
            return;
        }
        
        Pipe<Element, Object> pipe = null;
        if (queryCache.containsKey(query)) {
            pipe = queryCache.get(query);
        } else {
            try {
                pipe = Gremlin.compile(query);
            } catch (Exception e) {
                sendError(message, "Action 'query': Cannot compile query.", e);
                return;
            }
            
            if (message.body().getBoolean("cache", true)) {
                queryCache.put(query, pipe);
            }
        }
        
        pipe.setStarts(new SingleIterator<Element>(element));
        
        JsonArray queryResults;
        try {
            queryResults = jsonUtility.serializePipe(pipe);
        } catch (IOException e) {
            sendError(message, "Action 'query': Error converting Pipe to JSON.", e);
            return;
        }
        
        JsonObject reply = new JsonObject();
        reply.putArray("results", queryResults);

        sendOK(message, reply);
    }
    
    /**
     * Retrieve one or more vertices from the db in a single call. The {@link Message} 
     * may contain optional 'key' and a 'value' fields to filter only on those vertices that
     * have the specified key/value pair.<p/>
     * 
     * @param message the message containing information on the vertices to retrieve
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    protected void getVertices(Message<JsonObject> message, Graph graph) {
        String key = message.body().getString("key");
        Object value = message.body().getValue("value");
        
        JsonArray verticesJson;
        try {
            if (key == null) {
                verticesJson = jsonUtility.serializeElements(graph.getVertices());    
            } else if (value != null) {
                verticesJson = jsonUtility.serializeElements(graph.getVertices(key, value));
            } else {
                sendError(message, "Action 'getVertices': Both a key and a value must be specified");
                return;
            }
        } catch (IOException e) {
            sendError(message, "Action 'getVertices': Cannot convert vertices to JSON", e);
            return;            
        }
        
        JsonObject reply = new JsonObject()
                .putString("mode", jsonUtility.getGraphSONMode())
                .putArray("vertices", verticesJson);
        
        sendOK(message, reply);
    }
    
    /**
     * Retrieve the {@link Vertex} with the id specified in the {@link Message}.<p/>
     * 
     * @param message the message containing information on the Vertex to retrieve
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    protected void getVertex(Message<JsonObject> message, Graph graph) {
        getElement(message, graph, "Vertex");
    }
    
    /**
     * Remove the {@link Vertex} with the id specified in the {@link Message} from the database.
     * <p/>
     * 
     * @param message the message containing information on the Vertex to remove
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    protected void removeVertex(Message<JsonObject> message, Graph graph) {
        removeElement(message, graph, "Vertex");
    }
    
    /**
     * Add a new {@link Edge} to the db and return a reply  with the Id of the 
     * newly created edge. The edge in the message body must follow the GraphSON format.
     * Only the first Edge in the GraphSON message is processed, other edges and
     * vertices are ignored.<p/>
     * 
     * The vertices for both the _inV and _outV vertex id's specified in the GraphSON message
     * must both exist in the db.
     * 
     * Note that the id is not guaranteed to be similar to that received in the incoming,
     * {@link Message} since Id generation may be database-vendor-specific.<p/>
     * 
     * If an error occurs and the graph was transactional, then a rollback will occur.<p/>
     * 
     * @param message the message containing information on the new Edge to create
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    protected void addEdge(Message<JsonObject> message, Graph graph) {

        // Formatted according to GraphSON format.
        JsonArray edgesJson = message.body().getArray("edges");
        JsonObject edgeJson = edgesJson.get(0);
        Object inId = edgeJson.getField("_inV");
        Object outId = edgeJson.getField("_outV");
        
        // The label is a required field in some database products.
        if (edgeJson.getString("_label") == null) {
            sendError(message, "Action 'addEdge': Key _label is a required field");
            return;
        }
        
        Vertex inVertex = graph.getVertex(inId);
        Vertex outVertex = graph.getVertex(outId);
        Edge edge;
        
        try {
            edge = jsonUtility.deserializeEdge(graph, inVertex, outVertex, edgeJson);
        } catch (IOException e) {
            sendError(message, "Action 'addEdge': The Graphson message is invalid", e);
            return;
        }
        
        if (graph instanceof TransactionalGraph) {
            ((TransactionalGraph) graph).commit();
        } else if (graph.getFeatures().ignoresSuppliedIds) {
            // Shutting down the graph should force Id generation.
            graph.shutdown();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Added Edge with Id: " + edge.getId().toString());
        }
        
        JsonObject reply = new JsonObject().putValue("_id", edge.getId());

        sendOK(message, reply);
    }
    
    /**
     * Retrieve the {@link Edge} with the id specified in the {@link Message}.<p/>
     * 
     * @param message the message containing information on the Edge to retrieve
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    protected void getEdge(Message<JsonObject> message, Graph graph) {
        getElement(message, graph, "Edge");
    }
    
    /**
     * Retrieve one or more edges from the db in a single call. The {@link Message} 
     * may contain optional 'key' and a 'value' fields to filter only on those edges that
     * have the specified key/value pair.<p/>
     * 
     * @param message the message containing information on the edges to retrieve
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    protected void getEdges(Message<JsonObject> message, Graph graph) {
        String key = message.body().getString("key");
        Object value = message.body().getValue("value");
        
        JsonArray edges;
        try {
            if (key == null) {
                edges = jsonUtility.serializeElements(graph.getEdges());    
            } else if (value != null) {
                edges = jsonUtility.serializeElements(graph.getEdges(key, value));
            } else {
                sendError(message, "Action 'getEdges': Both a key and a value must be specified");
                return;
            }
        }
        catch (IOException e) {
            sendError(message, "Action 'getEdges': Cannot convert Edges to JSON", e);
            return;
        }
        
        JsonObject reply = new JsonObject()
                .putString("mode", jsonUtility.getGraphSONMode())
                .putArray("edges", edges);
        
        sendOK(message, reply);
    }
    
    /**
     * Remove the {@link Edge} with the id specified in the {@link Message} from the database.
     * <p/>
     * 
     * @param message the message containing information on the Edge to remove
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    protected void removeEdge(Message<JsonObject> message, Graph graph) {
        removeElement(message, graph, "Edge");
    }
        
    /**
     * Create an index in the underlying graph database based on the provided key and optional
     * parameters.
     * 
     * @param message the message containing information on the Key Index to create
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    protected void createKeyIndex(Message<JsonObject> message, Graph graph) {
        
        if (graph.getFeatures().supportsKeyIndices && graph instanceof KeyIndexableGraph) {
            String key = getMandatoryString("key", message);
            
            Class<? extends Element> elementClass = getIndexElementClass(message);
            if (elementClass == null) {
                sendError(message, "Action 'createKeyIndex': Unsupported elementClass " + elementClass);
                return;
            }
            
            Parameter<String, Object>[] parameters = getIndexParameters(message);

            try {
                if (parameters == null) {
                    ((KeyIndexableGraph) graph).createKeyIndex(key, elementClass);
                } else {
                    ((KeyIndexableGraph) graph).createKeyIndex(key, elementClass, parameters);
                }
            } catch (RuntimeException e) {
                sendError(message, "Action 'createKeyIndex': Cannot create index with key " + key, e);
            }
            
            sendOK(message);
        } else {
            sendError(message, "Action 'createKeyIndex': Graph does not support key indices");
        }
    }

    /**
     * Drop an index in the underlying graph database based on the provided key. The available
     * index keys can be found by first sending a 'getIndexedKeys' action to the persistor.
     * 
     * @param message the message containing information on the Key Index to create
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    protected void dropKeyIndex(Message<JsonObject> message, Graph graph) {
        
        if (graph.getFeatures().supportsKeyIndices && graph instanceof KeyIndexableGraph) {
            String key = getMandatoryString("key", message);

            Class<? extends Element> elementClass = getIndexElementClass(message);
            if (elementClass == null) {
                sendError(message, "Action 'dropKeyIndex': Unsupported elementClass " + elementClass);
                return;
            }
            
            try {
                ((KeyIndexableGraph) graph).dropKeyIndex(key, elementClass);
            } catch (RuntimeException e) {
                sendError(message, "Action 'dropKeyIndex': Cannot drop index with key " + key, e);
            }
            
            sendOK(message);
        } else {
            sendError(message, "Action 'dropKeyIndex': Graph does not support key indices");
        }
    }
    
    /**
     * Get the list of keys for all of the indices that exist in the underlying graph database.
     * 
     * @param message the message that contains the element class for which to retrieve the indices
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    protected void getIndexedKeys(Message<JsonObject> message, Graph graph) {
        
        if (graph.getFeatures().supportsKeyIndices && graph instanceof KeyIndexableGraph) {
            try {
                Class<? extends Element> elementClass = getIndexElementClass(message);
                if (elementClass == null) {
                    sendError(message, "Action 'dropKeyIndex': Unsupported elementClass " + elementClass);
                    return;
                }
                
                Set<String> indexedKeys = ((KeyIndexableGraph) graph).getIndexedKeys(elementClass);
                JsonObject reply = new JsonObject()
                        .putArray("keys", new JsonArray(indexedKeys.toArray()));
                
                sendOK(message, reply);
            } catch (RuntimeException e) {
                sendError(message, "Action 'getIndexedKeys': Cannot retrieve indexed keys", e);    
            }
        } else {
            sendError(message, "Action 'getIndexedKeys': Graph does not support key indices");
        }
    }
    
    protected void flushQueryCache(Message<JsonObject> message, Graph graph) {
        String query = message.body().getString("query");
        if (query == null) {
            queryCache.clear();
        } else {
            queryCache.remove(query);
        }
        sendOK(message);
    }
    
    private Class<? extends Element> getIndexElementClass(Message<JsonObject> message) {
        String elementClass = getMandatoryString("elementClass", message);

        switch (elementClass) {
            case "Vertex":
                return Vertex.class;
            case "Edge":
                return Edge.class;
            default:
                return null;
        }        
    }
    
    @SuppressWarnings("unchecked")
    private Parameter<String, Object>[] getIndexParameters(Message<JsonObject> message) {
        JsonObject indexParameters = message.body().getObject("parameters");
        
        Parameter<String, Object>[] parameters = null;
        if (indexParameters != null && indexParameters.size() > 0) {
            parameters = (Parameter<String, Object>[]) indexParameters.toMap().entrySet().toArray();
        }
        
        return parameters;
    }
    
    private void getElement(Message<JsonObject> message, 
            final Graph graph, String elementType) {
        
        Object id = getMandatoryValue(message, "_id");
        if (id == null) {
            return;
        }
        
        Element element = elementType.equals("Vertex") ? graph.getVertex(id) : graph.getEdge(id);
        if (element ==  null) {
            sendError(message, 
                    String.format("Action 'get%s': %s %s not found", 
                    elementType, elementType, id.toString()));
            return;
        }
        
        JsonObject elementJson;
        try {
            elementJson = jsonUtility.serializeElement(element);
        } catch (IOException e) {
            sendError(message, String.format(
                    "Action 'get%s': Cannot convert %s %s to JSON", 
                    elementType, elementType, element.toString()), e);
            return;
        }
        
        String arrayElement = elementType == "Vertex" ? "vertices" : "edges";
        JsonObject reply = new JsonObject()
                .putString("mode", jsonUtility.getGraphSONMode())
                .putArray(arrayElement, new JsonArray()
                        .addObject(elementJson));
        
        sendOK(message, reply);
    }
    
    private void removeElement(Message<JsonObject> message, 
            final Graph graph, String elementType) {
        
        Object id = getMandatoryValue(message, "_id");
        if (id == null) {
            return;
        }
        
        Element element = elementType.equals("Vertex") ? graph.getVertex(id) : graph.getEdge(id);
        if (element ==  null) {
            sendError(message, 
                    String.format("Action 'remove%s': Cannot remove. %s %s not found", 
                    elementType, elementType, id.toString()));
            return;
        }
        
        try {
            if (elementType.equals("Vertex")) {
                graph.removeVertex((Vertex) element);
            } else {
                graph.removeEdge((Edge) element);
            }
        } catch (Exception e) {
            sendError(message, 
                    String.format("Action 'remove%s': Error removing %s with Id %s", 
                    elementType, elementType, id.toString()));
            return;
        }

        if (graph instanceof TransactionalGraph) {
            ((TransactionalGraph) graph).commit();
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Removed %s with Id %s", elementType, id.toString()));
        }
        
        JsonObject reply = new JsonObject().putValue("_id", id);
        sendOK(message, reply);
    }
    
    /**
     * Load information on the graph database to connect to from the mod.json into
     * a {@link Configuration} object needed for opening the Tinkerpop {@link Graph}.<p/>
     * 
     * @return the graph database configuration
     */
    private Configuration loadTinkerpopConfig() {
        JsonObject tinkerpopConfigJson = config.getObject("tinkerpopConfig");
        if (tinkerpopConfigJson == null) {
            throw new IllegalArgumentException(
                    "tinkerpopConfig section must be specified in config");
        }
        
        Configuration config = new MapConfiguration(tinkerpopConfigJson.toMap());
        
        return config;
    }
    
    private Object getMandatoryValue(Message<JsonObject> message, String fieldName) {
        Object value = message.body().getField(fieldName);
        if (value == null) {
            String action = message.body().getString("action");
            sendError(message, 
                    String.format("Action '%s': %s must be specified", action, fieldName));
        }
        return value;
    }
}
