package org.apache.drill.exec.store.rest.helpers;

import com.github.jknack.handlebars.Formatter;
import com.github.jknack.handlebars.Handlebars;

import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang3.Validate.notNull;

/**
 * @author Oleg Zinoviev
 * @since 22.03.18.
 */
public class FixHandlebars extends Handlebars {

    /** List of formatters. */
    private List<Formatter> formatters = new ArrayList<>();


    @Override
    public Handlebars with(Formatter formatter) {
        notNull(formatter, "A formatter is required.");

        formatters.add(formatter);
        return this;
    }

    @Override
    public Formatter.Chain getFormatter() {
        return new FixChain(formatters);
    }

    private final static class FixChain implements Formatter.Chain {
        private final List<Formatter> formatters;

        FixChain(List<Formatter> formatters) {
            this.formatters =  formatters;
        }

        @Override
        public Object format(Object value) {
            if (formatters.isEmpty()) {
                return Formatter.NOOP.format(value);
            } else {
                Formatter rootFormatter = formatters.get(0);
                return rootFormatter.format(value, new FixChain(formatters.subList(1, formatters.size())));
            }
        }
    }
}
