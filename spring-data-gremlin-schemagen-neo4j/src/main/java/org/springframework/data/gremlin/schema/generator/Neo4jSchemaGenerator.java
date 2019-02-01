package org.springframework.data.gremlin.schema.generator;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.gremlin.annotation.Index;
import org.springframework.data.gremlin.schema.GremlinSchema;
import org.neo4j.ogm.annotation.EndNode;
import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.RelationshipEntity;
import org.neo4j.ogm.annotation.StartNode;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.gremlin.schema.GremlinSchema;
import org.neo4j.ogm.annotation.Property;
import org.neo4j.ogm.annotation.Relationship;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * Created by gman on 3/08/15.
 */
public class Neo4jSchemaGenerator extends BasicSchemaGenerator implements AnnotatedSchemaGenerator {

    /**
     * Returns the Vertex name. By default the Class' simple name is used. If it is annotated with @RelationshipEntity and the type parameter is
     * not empty, then that is used.
     *
     * @param clazz The Class to find the name of
     * @return The vertex name of the class
     */
    @Override
    protected String getVertexName(Class<?> clazz) {

        String className = super.getVertexName(clazz);
        RelationshipEntity entity = AnnotationUtils.getAnnotation(clazz, RelationshipEntity.class);
        if (entity != null && !StringUtils.isEmpty(entity.type())) {
            className = entity.type();
        }

        return className;
    }

    @Override
    protected Field getIdField(Class<?> cls) throws SchemaGeneratorException {
        final Field[] idFields = { null };

        ReflectionUtils.doWithFields(cls, new ReflectionUtils.FieldCallback() {
            @Override
            public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {

                Id id = AnnotationUtils.getAnnotation(field, Id.class);
                if (id != null) {
                    idFields[0] = field;
                } else {
	                GraphId graphid = AnnotationUtils.getAnnotation(field, GraphId.class);
	                if (graphid != null) {
	                    idFields[0] = field;
	                }
                }
            }
        });
        if (idFields[0] == null) {
            try {
                idFields[0] = super.getIdField(cls);
            } catch (SchemaGeneratorException e) {
                throw new SchemaGeneratorException("Cannot generate schema as there is no ID field. You must have a field of type Long or String annotated with @Id or named 'id'.");
            }
        }
        return idFields[0];
    }

    @Override
    protected Index.IndexType getIndexType(Field field) {
        Index.IndexType index = super.getIndexType(field);
        if (index == null || index == Index.IndexType.NONE) {

            org.neo4j.ogm.annotation.Index indexed = AnnotationUtils.getAnnotation(field, org.neo4j.ogm.annotation.Index.class);
            if (indexed != null) {
                if (indexed.unique()) {
                    index = Index.IndexType.UNIQUE;
                } else {
                    index = Index.IndexType.NON_UNIQUE;
                }
            } else {
                index = Index.IndexType.NONE;
            }
        }
        return index;
    }

    @Override
    protected String getPropertyName(Field field, Field rootEmbeddedField, Class<?> schemaClass) {
        String name = super.getPropertyName(field, rootEmbeddedField, schemaClass);

        // If annotated with @GraphProperty, use the propertyName parameter of the annotation
        Property graphProperty = AnnotationUtils.getAnnotation(field, Property.class);

        if (graphProperty != null) {
            if (!StringUtils.isEmpty(graphProperty.name())) {
                name = graphProperty.name();
            }
        } else {
            Relationship relatedTo = AnnotationUtils.getAnnotation(field, Relationship.class);
            if (relatedTo != null) {

                if (!StringUtils.isEmpty(relatedTo.type())) {
                    name = relatedTo.type();
                }
                if (isAdjacentField(field.getType(), field)) {
                    String adjacentName = getVertexName(field.getType());
                    if (!adjacentName.equals(field.getType().getName())) {
                        name = adjacentName;
                    }
                }
            }
        }

        return name;
    }

    @Override
    protected boolean isLinkField(Class<?> cls, Field field) {
        if (isVertexClass(cls)) {

            Annotation[] annotations = AnnotationUtils.getAnnotations(field);
            for (Annotation annotation : annotations) {
                if (annotation instanceof Relationship) {// || annotation instanceof StartNode || annotation instanceof EndNode) {
                    return true;
                }
            }
        }
        return false;
        //        return (isVertexClass(cls) || isEdgeClass(cls)) && ((AnnotationUtils.getAnnotation(field, RelatedTo.class) != null) || AnnotationUtils.getAnnotation(field, RelatedToVia
        // .class) !=
        //                                                                                                                                       null);
    }

    @Override
    protected boolean isLinkViaField(Class<?> cls, Field field) {
        if (isEdgeClass(cls)) {

            Annotation[] annotations = AnnotationUtils.getAnnotations(field);
            for (Annotation annotation : annotations) {
                if (annotation instanceof Relationship) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    protected boolean isAdjacentField(Class<?> cls, Field field) {
        if (isVertexClass(cls)) {
            Annotation[] annotations = AnnotationUtils.getAnnotations(field);
            for (Annotation annotation : annotations) {
                if (annotation instanceof StartNode || annotation instanceof EndNode) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    protected boolean isAdjacentOutward(Class<?> cls, Field field) {
        StartNode startNode = AnnotationUtils.getAnnotation(field, StartNode.class);
        if (startNode != null) {
            return true;
        }

        EndNode endNode = AnnotationUtils.getAnnotation(field, EndNode.class);
        if (endNode != null) {
            return false;
        }

        return true;
    }

    @Override
    protected boolean isLinkOutward(Class<?> cls, Field field) {
        Relationship relatedTo = AnnotationUtils.getAnnotation(field, Relationship.class);
        if (relatedTo != null) {
            return relatedTo.direction().equals(Relationship.OUTGOING);
        }
        StartNode startNode = AnnotationUtils.getAnnotation(field, StartNode.class);
        if (startNode != null) {
            return true;
        }

        EndNode endNode = AnnotationUtils.getAnnotation(field, EndNode.class);
        if (endNode != null) {
            return false;
        }

        return true;
    }

    @Override
    protected boolean isCollectionField(Class<?> cls, Field field, GremlinSchema schema) {
        return super.isCollectionField(cls, field, schema) && AnnotationUtils.getAnnotation(field, Relationship.class) != null;
    }

    @Override
    protected boolean isCollectionViaField(Class<?> cls, Field field, GremlinSchema schema) {
        return super.isCollectionViaField(cls, field, schema) && AnnotationUtils.getAnnotation(field, Relationship.class) != null;
    }
    //    @Override
    //    protected boolean isLinkViaEdge(Class<?> cls, Field field) {
    //        return isEdgeClass(cls) && (AnnotationUtils.getAnnotation(field, RelatedToVia.class) != null);
    //    }

    @Override
    public Class<? extends Annotation> getVertexAnnotationType() {
        return NodeEntity.class;
    }

    @Override
    public Class<? extends Annotation> getEmbeddedAnnotationType() {
        return null;
    }

    @Override
    public Class<? extends Annotation> getEdgeAnnotationType() {
        return RelationshipEntity.class;
    }
}
