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

package eu.tradegrid.tinkerpop.persistor.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.GraphElementFactory;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReader;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;
import com.tinkerpop.pipes.Pipe;

/**
 * Simple JSON utility class for serializing/deserializing graph elements and which encapsulates
 * the Jettison dependencies used by Tinkerpop exposing only Vert.x Json object instances. 
 * 
 * @author <a href="https://github.com/aschrijver">Arnold Schrijver</a>
 */
public class JsonUtility {

    protected final GraphSONMode graphsonMode;
    
    public JsonUtility(String graphSONMode) {
        this.graphsonMode = GraphSONMode.valueOf(graphSONMode);
    }
    
    public String getGraphSONMode() {
        return graphsonMode.name();
    }
    
    public JsonObject serializeGraph(Graph graph) throws IOException {
        String graphJsonResult;
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            GraphSONWriter.outputGraph(graph, os, graphsonMode);
            graphJsonResult = os.toString("UTF-8");
        }
        
        return new JsonObject().putObject("graph", new JsonObject(graphJsonResult));
    }
    
    public void deserializeGraph(Graph graph, JsonObject graphJson) 
            throws UnsupportedEncodingException, IOException {
        try (InputStream is = 
                new ByteArrayInputStream(graphJson.toString().getBytes("UTF-8"))) {
            
            GraphSONReader.inputGraph(graph, is);
        }
    }
    
    public Vertex deserializeVertex(Graph graph, JsonObject vertexJson) throws IOException {
        Vertex vertex;
        try {
            GraphElementFactory factory =  new GraphElementFactory(graph);
            vertex = GraphSONUtility.vertexFromJson(
                    vertexJson.toString(), factory, graphsonMode, null);            
        } finally {}
        
        return vertex;
    }

    public Edge deserializeEdge(
            Graph graph, Vertex inVertex, Vertex outVertex, JsonObject edgeJson) throws IOException {
        Edge edge;
        try {
            GraphElementFactory factory =  new GraphElementFactory(graph);
            edge = GraphSONUtility.edgeFromJson(
                    edgeJson.toString(), inVertex, outVertex, factory, graphsonMode, null);            
        } finally {}
        
        return edge;
    }
    
    public JsonArray serializePipe(Pipe<Element, Object> pipe) throws IOException {
        return serializeElements((Iterable<Object>) pipe);
    }
    
    public <T extends Element> JsonObject serializeElement(T element) throws IOException {
        JSONObject elementJson;
        try {
            elementJson = GraphSONUtility.jsonFromElement(element, null, graphsonMode);
        } catch (JSONException e) {
            throw new IOException(e);
        } finally {}
        
        return new JsonObject(elementJson.toString());
    }
    
    public <T> JsonArray serializeElements(Iterable<T> vertices) throws IOException {
        JSONArray results = new JSONArray();
        try {
            convertToJson(vertices, results);
        } catch (JSONException e) {
            throw new IOException(e);
        }
        
        return new JsonArray(results.toString());
    }
    
    @SuppressWarnings("unchecked")
    private <T> void convertToJson(Iterable<T> items, JSONArray results) throws JSONException {
        for (T resultObject : items) {
            if (resultObject instanceof Element) {
                results.put(GraphSONUtility.jsonFromElement(
                        (Element) resultObject, null, graphsonMode));
            } else if (resultObject instanceof List) {
                convertToJson((Iterable<T>) resultObject, results);
            }
        }
    }
}
