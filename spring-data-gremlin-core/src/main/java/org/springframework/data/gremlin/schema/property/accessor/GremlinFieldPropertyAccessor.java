package org.springframework.data.gremlin.schema.property.accessor;

import org.springframework.data.gremlin.schema.LazyInitializationHandler;

import java.lang.reflect.Field;

/**
 * A concrete {@link AbstractGremlinFieldPropertyAccessor} for basic Fields.
 *
 * @author Gman
 */
public class GremlinFieldPropertyAccessor<V> extends AbstractGremlinFieldPropertyAccessor<V> {

    public GremlinFieldPropertyAccessor(Field field) {
        super(field);
    }

    public GremlinFieldPropertyAccessor(Field field, GremlinFieldPropertyAccessor parentAccessor) {
        super(field, parentAccessor);
    }

    @Override
    public V get(Object object) {
        try {
            LazyInitializationHandler.initProxy(object);
            object = getEmbeddedObject(object, false);
            V result = null;
            if (object != null) {
                result = (V) field.get(object);
            }
            return result;
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void set(Object object, V val) {

        try {
            object = getEmbeddedObject(object, true);
            if (object != null) {
                field.set(object, val);
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return "GremlinFieldPropertyAccessor{"
                + "field=" + field
                + ", embeddedAccessor=" + embeddedAccessor +
                '}';
    }
}
