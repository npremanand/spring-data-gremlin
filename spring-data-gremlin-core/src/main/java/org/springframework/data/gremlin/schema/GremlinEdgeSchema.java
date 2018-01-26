package org.springframework.data.gremlin.schema;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.springframework.data.gremlin.schema.property.GremlinAdjacentProperty;
import org.springframework.data.gremlin.schema.property.GremlinProperty;
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
public class GremlinEdgeSchema<V> extends GremlinSchema<V> {

    public GremlinEdgeSchema(Class<V> classType, GremlinSchema<? super V> superSchema) {
        super(classType, superSchema);
    }

    private GremlinAdjacentProperty outProperty;
    private GremlinAdjacentProperty inProperty;

    public void addProperty(GremlinProperty property) {
        super.addProperty(property);
        if (property instanceof GremlinAdjacentProperty) {
            GremlinAdjacentProperty adjacentProperty = (GremlinAdjacentProperty) property;
            if (adjacentProperty.getDirection() == Direction.OUT) {
                outProperty = adjacentProperty;
            } else {
                inProperty = adjacentProperty;
            }
        }
    }

    public GremlinAdjacentProperty getOutProperty() {
        return outProperty;
    }

    public GremlinAdjacentProperty getInProperty() {
        return inProperty;
    }
}
