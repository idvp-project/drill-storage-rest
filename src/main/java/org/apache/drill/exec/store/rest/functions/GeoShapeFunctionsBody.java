/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.rest.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.geojson.GeoJsonObject;
import org.geojson.GeoJsonObjectVisitor;
import org.geojson.GeometryCollection;
import org.geojson.LineString;
import org.geojson.LngLatAlt;
import org.geojson.MultiLineString;
import org.geojson.MultiPoint;
import org.geojson.MultiPolygon;
import org.geojson.Point;
import org.geojson.Polygon;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Oleg Zinoviev
 * @since 30.10.17
 **/
@SuppressWarnings("WeakerAccess")
public final class GeoShapeFunctionsBody {

    static final ObjectMapper mapper = new ObjectMapper();
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GeoShapeFunctionsBody.class);


    private GeoShapeFunctionsBody() {
    }

    public static Iterable<byte[]> convert(ValueHolder source, String path) {
        String src = FunctionsHelper.asString(source);

        Iterable<byte[]> result = SelectorFunctionsBody.JsonPathSelectorFuncBody.eval(src, path);
        Iterator<byte[]> iterator = result.iterator();
        GeoJsonVisitor geoJsonVisitor = new GeoJsonVisitor();
        return () -> new Iterator<byte[]>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public byte[] next() {
                try {
                    byte[] inner = iterator.next();
                    GeoJsonObject geoJsonObject = mapper.readValue(inner, GeoJsonObject.class);
                    if (geoJsonObject == null) {
                        return StringUtils.EMPTY.getBytes(StandardCharsets.UTF_8);
                    }

                    return geoJsonObject.accept(geoJsonVisitor);
                } catch (IOException e) {
                    throw UserException.functionError(e).message("GeoShapeFunctionsBody").build(logger);
                }
            }
        };
    }

    private static final class GeoJsonVisitor implements GeoJsonObjectVisitor<byte[]> {
        final DecimalFormat format;

        {
            format = new DecimalFormat("#");
            format.setMaximumFractionDigits(16);
        }


        @Override
        public byte[] visit(Point point) {
            return createPoint(point.getCoordinates()).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] visit(Polygon polygon) {
            List<String> border = getBorder(polygon);

            List<List<String>> holes = getHoles(polygon);

            JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);
            ArrayNode rootNode = jsonNodeFactory.arrayNode();

            ObjectNode shape = jsonNodeFactory.objectNode();
            shape.set("border", jsonNodeFactory.pojoNode(border));
            shape.set("holes", jsonNodeFactory.pojoNode(holes));
            rootNode.add(shape);

            try {
                return mapper.writeValueAsBytes(rootNode);
            } catch (IOException e) {
                throw UserException.functionError(e).build(logger);
            }
        }


        @Override
        public byte[] visit(MultiPolygon multiPolygon) {
            JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);
            ArrayNode rootNode = jsonNodeFactory.arrayNode();

            for (List<List<LngLatAlt>> coordinates : multiPolygon.getCoordinates()) {
                if (coordinates == null || coordinates.isEmpty()) {
                    continue;
                }

                Polygon polygon = new Polygon(coordinates.get(0));
                if (coordinates.size() > 1) {
                    List<List<LngLatAlt>> holes = coordinates.subList(1, coordinates.size());
                    for (List<LngLatAlt> hole : holes) {
                        polygon.addInteriorRing(hole);
                    }
                }

                List<String> border = getBorder(polygon);
                List<List<String>> holes = getHoles(polygon);

                ObjectNode shape = jsonNodeFactory.objectNode();
                shape.set("border", jsonNodeFactory.pojoNode(border));
                shape.set("holes", jsonNodeFactory.pojoNode(holes));
                rootNode.add(shape);
            }

            try {
                return mapper.writeValueAsBytes(rootNode);
            } catch (IOException e) {
                throw UserException.functionError(e).build(logger);
            }
        }

        @Override
        public byte[] visit(GeometryCollection geometryCollection) {
            return StringUtils.EMPTY.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] visit(FeatureCollection featureCollection) {
            return StringUtils.EMPTY.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] visit(Feature feature) {
            return StringUtils.EMPTY.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] visit(MultiLineString multiLineString) {
            return StringUtils.EMPTY.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] visit(MultiPoint multiPoint) {
            return StringUtils.EMPTY.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] visit(LineString lineString) {
            return StringUtils.EMPTY.getBytes(StandardCharsets.UTF_8);
        }

        private String createPoint(LngLatAlt coordinates) {
            return format.format(coordinates.getLatitude()) + "," + format.format(coordinates.getLongitude());
        }

        private List<List<String>> getHoles(Polygon polygon) {
            List<List<String>> holes = new ArrayList<>(polygon.getInteriorRings().size());
            for (List<LngLatAlt> geoHole : polygon.getInteriorRings()) {
                List<String> hole = new ArrayList<>(geoHole.size());
                for (LngLatAlt point : geoHole) {
                    hole.add(createPoint(point));
                }
                holes.add(hole);
            }
            return holes;
        }

        private List<String> getBorder(Polygon polygon) {
            List<String> border = new ArrayList<>(polygon.getExteriorRing().size());
            for (LngLatAlt point : polygon.getExteriorRing()) {
                border.add(createPoint(point));
            }
            return border;
        }
    }
}
