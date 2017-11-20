package org.springframework.data.gremlin.query.orientdb;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.springframework.data.domain.Pageable;
import org.springframework.data.gremlin.query.AbstractNativeGremlinQuery;
import org.springframework.data.gremlin.query.GremlinQueryMethod;
import org.springframework.data.gremlin.repository.GremlinGraphAdapter;
import org.springframework.data.gremlin.schema.GremlinSchema;
import org.springframework.data.gremlin.schema.GremlinSchemaFactory;
import org.springframework.data.gremlin.tx.GremlinGraphFactory;
import org.springframework.data.gremlin.tx.orientdb.OrientDBGremlinGraphFactory;
import org.springframework.data.repository.query.DefaultParameters;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.util.Assert;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * Concrete {@link AbstractNativeGremlinQuery} handling OrientDB native queries.
 *
 * @author Gman
 */
public class NativeOrientdbGremlinQuery extends AbstractNativeGremlinQuery {
    private OrientDBGremlinGraphFactory orientGraphFactory;
    private SimpleDateFormat formatter;

    public NativeOrientdbGremlinQuery(GremlinGraphFactory dbf,
                                      GremlinQueryMethod method,
                                      GremlinSchemaFactory schemaFactory,
                                      GremlinGraphAdapter graphAdapter,
                                      String query) {
        super(dbf, method, schemaFactory, graphAdapter, query);
        this.orientGraphFactory = (OrientDBGremlinGraphFactory) dbf;
    }

    @Override
    protected Object doRunQuery(DefaultParameters parameters, Object[] values, boolean ignorePaging) {
        String queryString = query;
        Map<String, Object> params = new HashMap<>();
        if (parameters != null) {
            for (Object obj : parameters) {
                Parameter param = (Parameter) obj;
                Object val = values[param.getIndex()];
                if (val == null || val instanceof Pageable) {
                    continue;
                }
                String paramName = param.getName();
                String placeholder = param.getPlaceholder();
                if (paramName == null) {
                    paramName = "placeholder_" + param.getIndex();
                    queryString = queryString.replaceFirst("\\?", paramName);
                } else {
                    queryString = queryString.replaceFirst(placeholder, paramName);
                }
                params.put(paramName, val);
            }
            // TODO: Hack until OLuceneSpatialIndexManager.searchIntersect handles the context
            //        if (queryString.contains("$spatial")) {
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                String escapedValue = Matcher.quoteReplacement(convertObject(entry.getValue()));
                queryString = queryString.replaceAll("\\b" + entry.getKey() + "\\b", escapedValue);
            }
            //        }
            ParametersParameterAccessor accessor = new ParametersParameterAccessor(parameters, values);
            Pageable pageable = accessor.getPageable();
            if (pageable != null && !ignorePaging) {
                queryString = String.format("%s SKIP %d LIMIT %d", queryString, pageable.getOffset(), pageable.getPageSize());
            }
        }
        return run(queryString, params);
    }

    private Object run(String queryString, Map<String, Object> params) {
        Assert.hasLength(queryString);
        try {
            return orientGraphFactory.graph().command(new OCommandSQL(queryString)).execute(params);
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        }
    }

    private String escape(String input) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            switch (c) {
                case '\'':
                    builder.append("\\'");
                    break;
                case '\\':
                    builder.append("\\");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                default:
                    builder.append(c);
            }
        }
        return builder.toString();
    }

    // TODO: Hack until OLuceneSpatialIndexManager.searchIntersect handles the context
    private String convertObject(Object val) {
        if (val instanceof Date) {
            if (formatter == null) {
                formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }
            return "'" + formatter.format(val) + "'";
        } else if (val instanceof String) {
            return "'" + escape((String) val) + "'";
        } else if (val instanceof Double) {
            return val + "d";
        } else if (val instanceof Float) {
            return val + "f";
        } else {
            GremlinSchema schema = schemaFactory.getSchema(val.getClass());
            if (schema != null) {
                return schema.getGraphId(val);
            } else {
                return val.toString();
            }
        }
    }
}
