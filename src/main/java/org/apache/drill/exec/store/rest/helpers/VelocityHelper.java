package org.apache.drill.exec.store.rest.helpers;

import org.apache.drill.exec.store.rest.config.RuntimeQueryConfig;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.context.InternalContextAdapterImpl;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.RuntimeInstance;
import org.apache.velocity.runtime.log.NullLogChute;
import org.apache.velocity.runtime.parser.node.ASTReference;
import org.apache.velocity.runtime.parser.node.Node;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

import java.io.StringWriter;
import java.util.*;

/**
 * @author Oleg Zinoviev
 * @since 21.06.2017.
 */
public final class VelocityHelper {

    private final static String ENCODING = "utf-8";

    public final static VelocityHelper INSTANCE = new VelocityHelper();

    private final RuntimeInstance engine;

    private VelocityHelper() {
        Properties p = new Properties();
        p.put(RuntimeConstants.RESOURCE_LOADER, "classpath");
        p.put("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
        p.put(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, NullLogChute.class.getName());
        p.put(RuntimeConstants.INPUT_ENCODING, ENCODING);
        p.put(RuntimeConstants.OUTPUT_ENCODING, ENCODING);
        p.put(RuntimeConstants.RUNTIME_REFERENCES_STRICT, "true");

        engine = new RuntimeInstance();
        engine.init(p);
    }

    public Set<String> parameters(RuntimeQueryConfig queryConfig) {
        Set<String> result = new HashSet<>();
        result.addAll(parameters(queryConfig.getUrl()));
        result.addAll(parameters(queryConfig.getBaseUrl()));
        result.addAll(parameters(queryConfig.getBody()));
        return result;
    }

    public String merge(String input, Map<String, Object> parameters) {
        VelocityContext context = new VelocityContext(parameters == null ? Collections.emptyMap() : parameters);
        try (StringWriter writer = new StringWriter()) {
            engine.evaluate(context, writer, VelocityHelper.class.getName(), input);
            writer.flush();

            return writer.toString();
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        }

    }

    private Set<String> parameters(String input) {
        if (input == null) {
            return Collections.emptySet();
        }

        try {
            Set<String> result = new HashSet<>();
            Node node = engine.parse(input, "query");
            node.init(new InternalContextAdapterImpl(new VelocityContext()), engine);
            findParametersReference(node, result);
            return result;
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private void findParametersReference(Node node, Set<String> parameters) {
        if (node instanceof ASTReference) {
            ASTReference reference = (ASTReference) node;
            parameters.add(reference.getRootString());
        } else {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                findParametersReference(node.jjtGetChild(i), parameters);
            }
        }
    }
}
