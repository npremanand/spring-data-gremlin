package org.springframework.data.gremlin.schema;

import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyObject;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.gremlin.repository.GremlinGraphAdapter;
import org.springframework.data.gremlin.schema.property.accessor.GremlinPropertyAccessor;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * @author <a href="mailto:atul.mahind@kiwigrid.com">Atul Mahind</a>
 */
public class LazyInitializationHandler implements MethodHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LazyInitializationHandler.class);

    private volatile boolean initialized;

    private final GremlinSchema<?> schema;
    private final GremlinGraphAdapter graphAdapter;
    private final Element element;
    private final Map<Object, Object> noCascadingMap;

    LazyInitializationHandler(GremlinSchema<?> schema, GremlinGraphAdapter graphAdapter, Element element, Map<Object, Object> noCascadingMap) {
        this.schema = schema;
        this.graphAdapter = graphAdapter;
        this.element = element;
        this.noCascadingMap = noCascadingMap;
    }

    @Override
    public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
        //LOGGER.trace("Invoke {}", thisMethod);
        if (thisMethod.getDeclaringClass() == Object.class) {
            if (args.length == 0) {
                if (thisMethod.getName().equals("hashCode")) {
                    return System.identityHashCode(self);
                }
                if (thisMethod.getName().equals("finalize")) {
                    return proceed.invoke(self, args);
                }
            } else if (args.length == 1 && thisMethod.getName().equals("equals")) {
                return args[0] == self;
            }
        }
        init(self);
        return proceed.invoke(self, args);
    }

    private void init(Object self) {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    initialized = true;
                    this.initialize(self);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void initialize(Object self) {
        LOGGER.debug("Init proxy of {}:{} (keys: {})", schema.getClassName(), element.id(), element.keys());
        if (element instanceof Vertex) {
        	((Vertex)element).edges(Direction.OUT).forEachRemaining(e -> {
                LOGGER.debug("  edge this -[{}]-> {}", e.label(), e.inVertex());
        	});
        	((Vertex)element).edges(Direction.IN).forEachRemaining(e -> {
                LOGGER.debug("  edge this <-[{}]- {}", e.label(), e.outVertex());
        	});
            LOGGER.debug("Vertex links: {}:{} (keys: {})", schema.getClassName(), element.id(), element.keys());
        }
        try {
            GremlinPropertyAccessor idAccessor = schema.getIdAccessor();
            idAccessor.set(self, schema.encodeId(element.id().toString()));
        } catch (Exception e) {
            throw new IllegalStateException("Could not instantiate new " + schema.getClassType(), e);
        }
        schema.getPropertyStream().forEach(property -> {
            LOGGER.trace("Load property {}::{} of {}", schema.getClassType(), property.getName(), element.id());
            Object val = property.loadFromVertex(graphAdapter, element, noCascadingMap);
            LOGGER.trace("Loaded property {}::{} of {} : {}", schema.getClassType(), property.getName(), element.id(), val instanceof Collection ? "[" + ((Collection)val).size() + "]" : (val == null ? "<null>" : getClass().getSimpleName()));
            GremlinPropertyAccessor accessor = property.getAccessor();
            try {
                accessor.set(self, val);
            } catch (Exception e) {
                LOGGER.warn("Could not load property {} of {}", property, self.toString(), e);
            }
        });
        LOGGER.debug("Finished proxy initialization of {}:{}", schema.getClassName(), element.id());
    }

    public static void initProxy(Object object) {
        if (object instanceof ProxyObject) {
            MethodHandler handler = ((ProxyObject) object).getHandler();
            if (handler instanceof LazyInitializationHandler) {
                ((LazyInitializationHandler) handler).init(object);
            }
        }
    }

    public static boolean isInitialized(Object object) {
        if (object instanceof ProxyObject) {
            MethodHandler handler = ((ProxyObject) object).getHandler();
            if (handler instanceof LazyInitializationHandler) {
                return ((LazyInitializationHandler) handler).initialized;
            }
        }
        return true;
    }
}
