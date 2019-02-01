package org.springframework.data.gremlin.schema.writer.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.gremlin.schema.GremlinSchema;
import org.springframework.data.gremlin.schema.property.GremlinProperty;
import org.springframework.data.gremlin.schema.writer.AbstractSchemaWriter;
import org.springframework.data.gremlin.schema.writer.SchemaWriter;
import org.springframework.data.gremlin.schema.writer.SchemaWriterException;
import org.springframework.data.gremlin.tx.GremlinGraphFactory;
import org.springframework.data.gremlin.tx.orientdb.OrientDBGremlinGraphFactory;

import static org.springframework.data.gremlin.schema.property.GremlinRelatedProperty.CARDINALITY;

import java.io.Closeable;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;

/**
 * A concrete {@link SchemaWriter} for an OrientDB database.
 *
 * @author Gman
 */
public class OrientDbSchemaWriter extends AbstractSchemaWriter<OClass, OClass, OProperty, OClass> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrientDbSchemaWriter.class);

    private OrientDBGremlinGraphFactory dbf;
    private OrientGraph graph;
    private OSchema oSchema;
    private OClass v;
    private OClass e;

    public boolean initialise(GremlinGraphFactory tgf, GremlinSchema<?> schema) throws SchemaWriterException {
    	if (this.oSchema != null) {
    		return false; // already initialized, leave it be
    	}
        LOGGER.debug("Initialising...");
        try {
            dbf = (OrientDBGremlinGraphFactory) tgf;
            graph = dbf.graphNoTx();
			oSchema = graph.getRawDatabase().getMetadata().getSchema();
        } catch (RuntimeException e) {
            String msg = String.format("Could not create schema %s. ERROR: %s", schema, e.getMessage());
            throw new SchemaWriterException(msg, e);
        }
        try {

            v = graph.getRawDatabase().getClass(OClass.VERTEX_CLASS_NAME);
            e = graph.getRawDatabase().getClass(OClass.EDGE_CLASS_NAME);

        } catch (Exception e) {

            // If any exception, drop the class
            try {
                oSchema.dropClass(schema.getClassName());
            } catch (Exception e1) {
                // Ignore
            }

            String msg = String.format("Could not create schema %s. ERROR: %s", schema, e.getMessage());
            LOGGER.error(e.getMessage(), e);
            throw new SchemaWriterException(msg, e);
        }
        LOGGER.debug("Initialised.");
        return true;
    }

    void cleanup() {
    	dbf = null;
    	if (graph != null) {
    		graph.close(); // releases it back to the pool
    		graph = null;
    	}
    	oSchema = null;
    	v = null;
    	e = null;
    }
    
    @Override
    public void writeSchema(GremlinGraphFactory tgf, GremlinSchema<?> schema) throws SchemaWriterException {
        boolean didInitialize = initialise(tgf, schema);
        try {
        	super.writeSchema(tgf, schema);
        } finally {
	    	if (didInitialize) {
	    		cleanup();
	    	}
    	}
    }

    @Override
    protected boolean isPropertyAvailable(Object vertexClass, String name) {
        OProperty prop = ((OClass) vertexClass).getProperty(name);
        return prop != null;
    }

    @Override
    protected OClass createVertexClass(GremlinSchema schema) throws Exception {
        OClass base = v;
        if (schema.getSuperSchema() != null) {
            base = createVertexClass(schema.getSuperSchema());
        }
        OClass oClass = getOrCreateClass(oSchema, base, schema.getClassName());
        oClass.setAbstract(schema.isAbstract());
        return oClass;
    }

    @Override
    protected OClass createEdgeClass(GremlinSchema schema) throws Exception {
        OClass base = e;
        if (schema.getSuperSchema() != null) {
            base = createEdgeClass(schema.getSuperSchema());
        }
        OClass oClass = getOrCreateClass(oSchema, base, schema.getClassName());
        oClass.setAbstract(schema.isAbstract());
        return oClass;
    }

    @Override
    protected void rollback(GremlinSchema schema) {

        // If any exception, drop the class
        try {
            oSchema.dropClass(schema.getClassName());
        } catch (Exception e1) {
            // Ignore
        }

    }

    @Override
    protected OClass createEdgeClass(String name, OClass outVertex, OClass inVertex, CARDINALITY cardinality) throws SchemaWriterException {
        OClass edgeClass = getOrCreateClass(oSchema, e, name);

        if (!edgeClass.existsProperty("out")) {
            OProperty out = edgeClass.createProperty("out", OType.LINK);
            out.setLinkedClass(outVertex);

            out.setMax("1");
            if (cardinality == CARDINALITY.ONE_TO_MANY) {
                out.setMax(String.valueOf(Integer.MAX_VALUE));
            }
        }

        if (!edgeClass.existsProperty("in")) {
            OProperty in = edgeClass.createProperty("in", OType.LINK);
            in.setLinkedClass(inVertex);

            in.setMax("1");
            if (cardinality == CARDINALITY.MANY_TO_ONE) {
                in.setMax(String.valueOf(Integer.MAX_VALUE));
            }
        }

        return edgeClass;
    }

    @Override
    protected boolean isEdgeInProperty(OClass edgeClass) {
        return edgeClass.getProperty("in") != null;
    }

    @Override
    protected boolean isEdgeOutProperty(OClass edgeClass) {
        return edgeClass.getProperty("out") != null;
    }

    @Override
    protected OProperty setEdgeOut(OClass edgeClass, OClass vertexClass) {
        OProperty out = edgeClass.createProperty("out", OType.LINK);
        out.setLinkedClass(vertexClass);
        return out;
    }

    @Override
    protected OProperty setEdgeIn(OClass edgeClass, OClass vertexClass) {
        OProperty in = edgeClass.createProperty("in", OType.LINK);
        in.setLinkedClass(vertexClass);
        return in;
    }

    @Override
    protected OProperty createProperty(OClass parentElement, String name, Class<?> cls) {
        OType oType = OType.getTypeByClass(cls);
        return parentElement.createProperty(name, oType);
    }

    @Override
    protected void createNonUniqueIndex(OProperty prop) {
        LOGGER.debug("createNonUniqueIndex:{}", prop);
        prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    }

    @Override
    protected void createUniqueIndex(OProperty prop) {
        LOGGER.debug("createUniqueIndex:{}", prop);
        prop.createIndex(OClass.INDEX_TYPE.UNIQUE);
    }

    @Override
    protected void createSpatialIndex(GremlinSchema<?> schema, GremlinProperty latitude, GremlinProperty longitude) {
        String indexName = schema.getClassName() + ".lat_lon";
        if (graph.getVertexIndexedKeys(indexName) == null) {
            try {
                Object result = graph.executeCommand(new OCommandSQL(String.format("CREATE INDEX %s ON %s(%s,%s) SPATIAL ENGINE LUCENE", indexName, schema.getClassName(), latitude.getName(), longitude.getName())));
                if (result instanceof Closeable) {
                	((Closeable)result).close();
                }
            } catch (Exception e1) {
                LOGGER.warn("createSpatialIndex: can't create index : " + indexName, e1);
                e1.printStackTrace();
                // TODO: Really swallow this exception?
            }
        } else {
            LOGGER.warn("createSpatialIndex:exists:{}", indexName);
        }
    }

    private OClass getOrCreateClass(OSchema oSchema, OClass superclass, String classname) throws SchemaWriterException {
        OClass newClass = oSchema.getOrCreateClass(classname, superclass);
        if (!newClass.getSuperClass().getName().equals(superclass.getName())) {
            String msg = String.format("Could not create %s '%s' of type %s. A conflicting %s exists of type %s", getClassType(superclass), classname, superclass.getName(), getClassType(superclass),
                newClass.getSuperClass().getName());
            throw new SchemaWriterException(msg);
        }
        return newClass;
    }

    private String getClassType(OClass type) {
        return (type.getName().equals("E")) ? "property" : "class";
    }
    //
    //    private void writeProperties(OrientDBGremlinGraphFactory dbf, OSchema oSchema, OClass vClass, OClass v, OClass e, GremlinSchema<?> schema) throws SchemaWriterException {
    //        GremlinProperty latitude = null;
    //        GremlinProperty longitude = null;
    //        for (GremlinProperty property : schema.getProperties()) {
    //
    //            Class<?> cls = property.getType();
    //
    //            try {
    //                OProperty prop = vClass.getProperty(property.getName());
    //
    //                // If prop is null, it does not exist, so let's create it
    //                if (prop == null) {
    //
    //                    // If this property is a LINK
    //                    if (property instanceof GremlinLinkProperty) {
    //
    //                        OClass eClass = getOrCreateClass(oSchema, e, property.getName());
    //
    //                        if (eClass.getProperty("out") == null) {
    //                            OProperty out = eClass.createProperty("out", OType.LINK);
    //                            out.setLinkedClass(vClass);
    //                        }
    //                        if (eClass.getProperty("in") == null) {
    //                            OProperty in = eClass.createProperty("in", OType.LINK);
    //                            OClass linkedClass = getOrCreateClass(oSchema, v, ((GremlinRelatedProperty) property).getRelatedSchema().getClassName());
    //                            in.setLinkedClass(linkedClass);
    //                        }
    //
    //                        break;
    //
    //                    } else if (property instanceof GremlinCollectionProperty) {
    //
    //                        OClass edgeClass = getOrCreateClass(oSchema, e, property.getName());
    //
    //                        if (edgeClass.getProperty("out") == null) {
    //                            OProperty out = edgeClass.createProperty("out", OType.LINK);
    //                            out.setLinkedClass(vClass);
    //                        }
    //                        if (edgeClass.getProperty("in") == null) {
    //                            OProperty in = edgeClass.createProperty("in", OType.LINK);
    //                            OClass linkedClass = getOrCreateClass(oSchema, v, ((GremlinRelatedProperty) property).getRelatedSchema().getClassName());
    //                            in.setLinkedClass(linkedClass);
    //                        }
    //
    //                        break;
    //
    //                    } else {
    //                        // Standard property, primitive, String, Enum, byte[]
    //                        OType oType = OType.getTypeByClass(cls);
    //                        if (oType != null) {
    //
    //                            prop = vClass.createProperty(property.getName(), oType);
    //                            switch (property.getIndex()) {
    //                            case UNIQUE:
    //                                prop.createIndex(OClass.INDEX_TYPE.UNIQUE);
    //                                break;
    //                            case NON_UNIQUE:
    //                                prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    //                                break;
    //                            case SPATIAL_LATITUDE:
    //                                latitude = property;
    //                                break;
    //
    //                            case SPATIAL_LONGITUDE:
    //                                longitude = property;
    //                                break;
    //                            }
    //                        }
    //                    }
    //                }
    //            } catch (OSchemaException e1) {
    //                LOGGER.warn(String.format("Could not create property %s of type %s", property, cls), e1);
    //            }
    //        }
    //
    //        if (latitude != null && longitude != null) {
    //            String indexName = schema.getClassName() + ".lat_lon";
    //            if (dbf.graphNoTx().getIndex(indexName, Vertex.class) == null) {
    //                try {
    //                    dbf.graphNoTx().command(new OCommandSQL(String.format("CREATE INDEX %s ON %s(%s,%s) SPATIAL ENGINE LUCENE", indexName, schema.getClassName(), latitude.getName(),
    //                                                                          longitude.getName()))).execute();
    //                } catch (Exception e1) {
    //                    e1.printStackTrace();
    //                }
    //
    //            }
    //        }
    //    }

}
