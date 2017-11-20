package org.springframework.data.gremlin.schema;

import org.springframework.data.gremlin.schema.property.accessor.GremlinFieldPropertyAccessor;


/**
 * <p>
 * Defines the schema of a mapped Class. Each GremlinSchema holds the {@code className}, {@code classType},
 * {@code schemaType} (VERTEX, EDGE) and the identifying {@link GremlinFieldPropertyAccessor}.
 * </p>
 * <p>
 * The GremlinSchema contains the high level logic for converting Vertices to mapped classes.
 * </p>
 *
 * @author Gman
 */
public class GremlinVertexSchema<V> extends GremlinSchema<V> {

    public GremlinVertexSchema(Class<V> classType, GremlinSchema<? super V> superSchema) {
        super(classType, superSchema);
    }

}
