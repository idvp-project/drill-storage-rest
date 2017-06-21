package org.apache.drill.exec.vector.complex.fn;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.store.easy.json.reader.BaseJsonProcessor;
import org.apache.drill.exec.store.rest.read.ReaderHelper;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * @author Oleg Zinoviev
 * @since 20.06.2017.
 */
public class RestJsonReader extends BaseJsonProcessor {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RestJsonReader.class);

    private final WorkingBufferProxy workingBuffer;
    private final List<SchemaPath> columns;
    private final boolean allTextMode;
    private final VectorOutput.MapVectorOutput mapOutput;
    private final VectorOutput.ListVectorOutput listOutput;
    private final boolean extended = true;
    private final boolean readNumbersAsDouble;

    /**
     * Collection for tracking empty array writers during reading
     * and storing them for initializing empty arrays
     */
    private final List<BaseWriter.ListWriter> emptyArrayWriters = Lists.newArrayList();

    /**
     * Describes whether or not this reader can unwrap a single root array record
     * and treat it like a set of distinct records.
     */
    private final boolean skipOuterList;

    /**
     * Фильтры, проброшенные в REST запрос
     */
    private final Map<String, Object> pushedDownFilters;

    /**
     * Whether the reader is currently in a situation where we are unwrapping an
     * outer list.
     */
    private boolean inOuterList;
    /**
     * The name of the current field being parsed. For Error messages.
     */
    private String currentFieldName;

    private FieldSelection selection;

    public RestJsonReader(DrillBuf managedBuf,
                          boolean allTextMode,
                          boolean skipOuterList,
                          boolean readNumbersAsDouble,
                          Map<String, Object> pushedDownFilters) {
        this(managedBuf, GroupScan.ALL_COLUMNS, allTextMode, skipOuterList,
                readNumbersAsDouble, pushedDownFilters);
    }

    private RestJsonReader(DrillBuf managedBuf,
                           List<SchemaPath> columns,
                           boolean allTextMode,
                           boolean skipOuterList,
                           boolean readNumbersAsDouble,
                           Map<String, Object> pushedDownFilters) {
        super(managedBuf);
        assert Preconditions.checkNotNull(columns).size() > 0 : "JSON record reader requires at least one column";
        this.selection = FieldSelection.getFieldSelection(columns);
        this.workingBuffer = new WorkingBufferProxy(managedBuf);
        this.skipOuterList = skipOuterList;
        this.allTextMode = allTextMode;
        this.columns = columns;
        this.mapOutput = new VectorOutput.MapVectorOutput(workingBuffer);
        this.listOutput = new VectorOutput.ListVectorOutput(workingBuffer);
        this.currentFieldName = "<none>";
        this.readNumbersAsDouble = readNumbersAsDouble;
        this.pushedDownFilters = pushedDownFilters == null ? Collections.emptyMap() : pushedDownFilters;
    }

    @SuppressWarnings("resource")
    @Override
    public void ensureAtLeastOneField(BaseWriter.ComplexWriter writer) {
        List<BaseWriter.MapWriter> writerList = Lists.newArrayList();
        List<PathSegment> fieldPathList = Lists.newArrayList();
        BitSet emptyStatus = new BitSet(columns.size());

        // first pass: collect which fields are empty
        for (int i = 0; i < columns.size(); i++) {
            SchemaPath sp = columns.get(i);
            PathSegment fieldPath = sp.getRootSegment();
            BaseWriter.MapWriter fieldWriter = writer.rootAsMap();
            while (fieldPath.getChild() != null && !fieldPath.getChild().isArray()) {
                fieldWriter = fieldWriter.map(fieldPath.getNameSegment().getPath());
                fieldPath = fieldPath.getChild();
            }
            writerList.add(fieldWriter);
            fieldPathList.add(fieldPath);
            if (fieldWriter.isEmptyMap()) {
                emptyStatus.set(i, true);
            }
            if (i == 0 && !allTextMode) {
                // when allTextMode is false, there is not much benefit to producing all
                // the empty
                // fields; just produce 1 field. The reason is that the type of the
                // fields is
                // unknown, so if we produce multiple Integer fields by default, a
                // subsequent batch
                // that contains non-integer fields will error out in any case. Whereas,
                // with
                // allTextMode true, we are sure that all fields are going to be treated
                // as varchar,
                // so it makes sense to produce all the fields, and in fact is necessary
                // in order to
                // avoid schema change exceptions by downstream operators.
                break;
            }

        }

        // second pass: create default typed vectors corresponding to empty fields
        // Note: this is not easily do-able in 1 pass because the same fieldWriter
        // may be
        // shared by multiple fields whereas we want to keep track of all fields
        // independently,
        // so we rely on the emptyStatus.
        for (int j = 0; j < fieldPathList.size(); j++) {
            BaseWriter.MapWriter fieldWriter = writerList.get(j);
            PathSegment fieldPath = fieldPathList.get(j);
            if (emptyStatus.get(j)) {
                if (allTextMode) {
                    fieldWriter.varChar(fieldPath.getNameSegment().getPath());
                } else {
                    fieldWriter.integer(fieldPath.getNameSegment().getPath());
                }
            }
        }

        for (BaseWriter.ListWriter field : emptyArrayWriters) {
            // checks that array has not been initialized
            if (field.getValueCapacity() == 0) {
                if (allTextMode) {
                    field.varChar();
                } else {
                    field.integer();
                }
            }
        }
    }

    @Override
    public void setSource(InputStream is) throws IOException {
        super.setSource(is);
        mapOutput.setParser(parser);
        listOutput.setParser(parser);
    }

    @Override
    public void setSource(JsonNode node) {
        super.setSource(node);
        mapOutput.setParser(parser);
        listOutput.setParser(parser);
    }

    @Override
    public ReadState write(BaseWriter.ComplexWriter writer) throws IOException {

        ReadState readState;
        try {
            JsonToken t = lastSeenJsonToken;
            if (t == null || t == JsonToken.END_OBJECT) {
                t = parser.nextToken();
            }
            while (!parser.hasCurrentToken() && !parser.isClosed()) {
                t = parser.nextToken();
            }
            lastSeenJsonToken = null;

            if (parser.isClosed()) {
                return ReadState.END_OF_STREAM;
            }

            readState = writeToVector(writer, t);

            switch (readState) {
                case END_OF_STREAM:
                    break;
                case WRITE_SUCCEED:
                    break;
                default:
                    throw getExceptionWithContext(UserException.dataReadError(),
                            currentFieldName, null).message(
                            "Failure while reading JSON. (Got an invalid read state %s )",
                            readState.toString()).build(logger);
            }
        } catch (com.fasterxml.jackson.core.JsonParseException ex) {
            if (ignoreJSONParseError()) {
                if (processJSONException() == JsonExceptionProcessingState.END_OF_STREAM) {
                    return ReadState.JSON_RECORD_PARSE_EOF_ERROR;
                } else {
                    return ReadState.JSON_RECORD_PARSE_ERROR;
                }
            } else {
                throw ex;
            }
        }
        return readState;
    }

    private void confirmLast() throws IOException {
        parser.nextToken();
        if (!parser.isClosed()) {
            throw getExceptionWithContext(UserException.dataReadError(),
                    currentFieldName, null)
                    .message(
                            "Drill attempted to unwrap a toplevel list "
                                    + "in your document.  However, it appears that there is trailing content after this top level list.  Drill only "
                                    + "supports querying a set of distinct maps or a single json array with multiple inner maps.")
                    .build(logger);
        }
    }

    private ReadState writeToVector(BaseWriter.ComplexWriter writer, JsonToken t)
            throws IOException {

        switch (t) {
            case START_OBJECT:
                writeDataSwitch(writer.rootAsMap());
                break;
            case START_ARRAY:
                if (inOuterList) {
                    throw getExceptionWithContext(UserException.dataReadError(),
                            currentFieldName, null)
                            .message(
                                    "The top level of your document must either be a single array of maps or a set "
                                            + "of white space delimited maps.").build(logger);
                }

                if (skipOuterList) {
                    t = parser.nextToken();
                    if (t == JsonToken.START_OBJECT) {
                        inOuterList = true;
                        writeDataSwitch(writer.rootAsMap());
                    } else {
                        throw getExceptionWithContext(UserException.dataReadError(),
                                currentFieldName, null)
                                .message(
                                        "The top level of your document must either be a single array of maps or a set "
                                                + "of white space delimited maps.").build(logger);
                    }

                } else {
                    writeDataSwitch(writer.rootAsList());
                }
                break;
            case END_ARRAY:

                if (inOuterList) {
                    confirmLast();
                    return ReadState.END_OF_STREAM;
                } else {
                    throw getExceptionWithContext(UserException.dataReadError(),
                            currentFieldName, null).message(
                            "Failure while parsing JSON.  Ran across unexpected %s.",
                            JsonToken.END_ARRAY).build(logger);
                }

            case NOT_AVAILABLE:
                return ReadState.END_OF_STREAM;
            default:
                throw getExceptionWithContext(UserException.dataReadError(),
                        currentFieldName, null)
                        .message(
                                "Failure while parsing JSON.  Found token of [%s].  Drill currently only supports parsing "
                                        + "json strings that contain either lists or maps.  The root object cannot be a scalar.",
                                t).build(logger);
        }

        return ReadState.WRITE_SUCCEED;

    }

    private void writeDataSwitch(BaseWriter.MapWriter w) throws IOException {
        if (this.allTextMode) {
            writeDataAllText(w, this.selection, true, true);
        } else {
            writeData(w, this.selection, true, true);
        }
    }

    private void writeDataSwitch(BaseWriter.ListWriter w) throws IOException {
        if (this.allTextMode) {
            writeDataAllText(w);
        } else {
            writeData(w);
        }
    }

    private void consumeEntireNextValue() throws IOException {
        switch (parser.nextToken()) {
            case START_ARRAY:
            case START_OBJECT:
                parser.skipChildren();
                return;
            default:
                // hit a single value, do nothing as the token was already read
                // in the switch statement
        }
    }

    /**
     *
     * @param moveForward
     *          Whether or not we should start with using the current token or the
     *          next token. If moveForward = true, we should start with the next
     *          token and ignore the current one.
     */
    private void writeData(BaseWriter.MapWriter map, FieldSelection selection,
                           boolean moveForward,
                           boolean root) throws IOException {
        //
        map.start();
        final Set<String> currentObjectFields = new HashSet<>();
        try {
            outside: while (true) {

                JsonToken t;
                if (moveForward) {
                    t = parser.nextToken();
                } else {
                    t = parser.getCurrentToken();
                    moveForward = true;
                }
                if (t == JsonToken.NOT_AVAILABLE || t == JsonToken.END_OBJECT) {
                    return;
                }

                assert t == JsonToken.FIELD_NAME : String.format(
                        "Expected FIELD_NAME but got %s.", t.name());

                final String fieldName = parser.getText();
                currentObjectFields.add(fieldName);
                this.currentFieldName = fieldName;
                FieldSelection childSelection = selection.getChild(fieldName);
                if (childSelection.isNeverValid()) {
                    consumeEntireNextValue();
                    continue;
                }

                switch (parser.nextToken()) {
                    case START_ARRAY:
                        writeData(map.list(fieldName));
                        break;
                    case START_OBJECT:
                        if (!writeMapDataIfTyped(map, fieldName)) {
                            writeData(map.map(fieldName), childSelection, false, false);
                        }
                        break;
                    case END_OBJECT:
                        break outside;

                    case VALUE_FALSE: {
                        map.bit(fieldName).writeBit(0);
                        break;
                    }
                    case VALUE_TRUE: {
                        map.bit(fieldName).writeBit(1);
                        break;
                    }
                    case VALUE_NULL:
                        // do nothing as we don't have a type.
                        break;
                    case VALUE_NUMBER_FLOAT:
                        map.float8(fieldName).writeFloat8(parser.getDoubleValue());
                        break;
                    case VALUE_NUMBER_INT:
                        if (this.readNumbersAsDouble) {
                            map.float8(fieldName).writeFloat8(parser.getDoubleValue());
                        } else {
                            map.bigInt(fieldName).writeBigInt(parser.getLongValue());
                        }
                        break;
                    case VALUE_STRING:
                        handleString(parser, map, fieldName);
                        break;

                    default:
                        throw getExceptionWithContext(UserException.dataReadError(),
                                currentFieldName, null).message("Unexpected token %s",
                                parser.getCurrentToken()).build(logger);
                }

            }
        } finally {
            if (root) {

                for (Map.Entry<String, Object> entry : pushedDownFilters.entrySet()) {
                    if (currentObjectFields.contains(entry.getKey()) || entry.getValue() == null) {
                        continue;
                    }

                    ReaderHelper.write(map, entry.getKey(), entry.getValue(), workingBuffer);
                }
            }

            currentObjectFields.clear();

            map.end();
        }

    }

    private void writeDataAllText(BaseWriter.MapWriter map, FieldSelection selection,
                                  boolean moveForward,
                                  boolean root) throws IOException {
        //
        map.start();
        final Set<String> currentObjectFields = new HashSet<>();
        outside: while (true) {

            JsonToken t;

            if (moveForward) {
                t = parser.nextToken();
            } else {
                t = parser.getCurrentToken();
                moveForward = true;
            }
            if (t == JsonToken.NOT_AVAILABLE || t == JsonToken.END_OBJECT) {
                return;
            }

            assert t == JsonToken.FIELD_NAME : String.format(
                    "Expected FIELD_NAME but got %s.", t.name());

            final String fieldName = parser.getText();
            currentObjectFields.add(fieldName);
            this.currentFieldName = fieldName;
            FieldSelection childSelection = selection.getChild(fieldName);
            if (childSelection.isNeverValid()) {
                consumeEntireNextValue();
                continue;
            }

            switch (parser.nextToken()) {
                case START_ARRAY:
                    writeDataAllText(map.list(fieldName));
                    break;
                case START_OBJECT:
                    if (!writeMapDataIfTyped(map, fieldName)) {
                        writeDataAllText(map.map(fieldName), childSelection, false, false);
                    }
                    break;
                case END_OBJECT:
                    break outside;

                case VALUE_EMBEDDED_OBJECT:
                case VALUE_FALSE:
                case VALUE_TRUE:
                case VALUE_NUMBER_FLOAT:
                case VALUE_NUMBER_INT:
                case VALUE_STRING:
                    handleString(parser, map, fieldName);
                    break;
                case VALUE_NULL:
                    // do nothing as we don't have a type.
                    break;

                default:
                    throw getExceptionWithContext(UserException.dataReadError(),
                            currentFieldName, null).message("Unexpected token %s",
                            parser.getCurrentToken()).build(logger);
            }
        }
        if (root) {
            for (Map.Entry<String, Object> entry : pushedDownFilters.entrySet()) {
                if (currentObjectFields.contains(entry.getKey()) || entry.getValue() == null) {
                    continue;
                }
                map.varChar(entry.getKey()).writeVarChar(0,
                        workingBuffer.prepareVarCharHolder(entry.getValue().toString()),
                        workingBuffer.getBuf());
            }
        }

        map.end();

    }

    /**
     * Will attempt to take the current value and consume it as an extended value
     * (if extended mode is enabled). Whether extended is enable or disabled, will
     * consume the next token in the stream.
     */
    @SuppressWarnings("ConstantConditions")
    private boolean writeMapDataIfTyped(BaseWriter.MapWriter writer, String fieldName)
            throws IOException {
        if (extended) {
            return mapOutput.run(writer, fieldName);
        } else {
            parser.nextToken();
            return false;
        }
    }

    /**
     * Will attempt to take the current value and consume it as an extended value
     * (if extended mode is enabled). Whether extended is enable or disabled, will
     * consume the next token in the stream.
     */
    @SuppressWarnings("ConstantConditions")
    private boolean writeListDataIfTyped(BaseWriter.ListWriter writer) throws IOException {
        if (extended) {
            return listOutput.run(writer);
        } else {
            parser.nextToken();
            return false;
        }
    }

    private void handleString(JsonParser parser, BaseWriter.MapWriter writer,
                              String fieldName) throws IOException {
        writer.varChar(fieldName).writeVarChar(0,
                workingBuffer.prepareVarCharHolder(parser.getText()),
                workingBuffer.getBuf());
    }

    private void handleString(JsonParser parser, BaseWriter.ListWriter writer)
            throws IOException {
        writer.varChar().writeVarChar(0,
                workingBuffer.prepareVarCharHolder(parser.getText()),
                workingBuffer.getBuf());
    }

    private void writeData(BaseWriter.ListWriter list) throws IOException {
        list.startList();
        outside: while (true) {
            try {
                switch (parser.nextToken()) {
                    case START_ARRAY:
                        writeData(list.list());
                        break;
                    case START_OBJECT:
                        if (!writeListDataIfTyped(list)) {
                            writeData(list.map(), FieldSelection.ALL_VALID, false, false);
                        }
                        break;
                    case END_ARRAY:
                        addIfNotInitialized(list);
                    case END_OBJECT:
                        break outside;

                    case VALUE_EMBEDDED_OBJECT:
                    case VALUE_FALSE: {
                        list.bit().writeBit(0);
                        break;
                    }
                    case VALUE_TRUE: {
                        list.bit().writeBit(1);
                        break;
                    }
                    case VALUE_NULL:
                        throw UserException
                                .unsupportedError()
                                .message(
                                        "Null values are not supported in lists by default. "
                                                + "Please set `store.json.all_text_mode` to true to read lists containing nulls. "
                                                + "Be advised that this will treat JSON null values as a string containing the word 'null'.")
                                .build(logger);
                    case VALUE_NUMBER_FLOAT:
                        list.float8().writeFloat8(parser.getDoubleValue());
                        break;
                    case VALUE_NUMBER_INT:
                        if (this.readNumbersAsDouble) {
                            list.float8().writeFloat8(parser.getDoubleValue());
                        } else {
                            list.bigInt().writeBigInt(parser.getLongValue());
                        }
                        break;
                    case VALUE_STRING:
                        handleString(parser, list);
                        break;
                    default:
                        throw UserException.dataReadError()
                                .message("Unexpected token %s", parser.getCurrentToken())
                                .build(logger);
                }
            } catch (Exception e) {
                throw getExceptionWithContext(e, this.currentFieldName, null).build(
                        logger);
            }
        }
        list.endList();

    }

    /**
     * Checks that list has not been initialized and adds it to the emptyArrayWriters collection.
     * @param list ListWriter that should be checked
     */
    private void addIfNotInitialized(BaseWriter.ListWriter list) {
        if (list.getValueCapacity() == 0) {
            emptyArrayWriters.add(list);
        }
    }

    private void writeDataAllText(BaseWriter.ListWriter list) throws IOException {
        list.startList();
        outside: while (true) {

            switch (parser.nextToken()) {
                case START_ARRAY:
                    writeDataAllText(list.list());
                    break;
                case START_OBJECT:
                    if (!writeListDataIfTyped(list)) {
                        writeDataAllText(list.map(), FieldSelection.ALL_VALID, false, false);
                    }
                    break;
                case END_ARRAY:
                    addIfNotInitialized(list);
                case END_OBJECT:
                    break outside;

                case VALUE_EMBEDDED_OBJECT:
                case VALUE_FALSE:
                case VALUE_TRUE:
                case VALUE_NULL:
                case VALUE_NUMBER_FLOAT:
                case VALUE_NUMBER_INT:
                case VALUE_STRING:
                    handleString(parser, list);
                    break;
                default:
                    throw getExceptionWithContext(UserException.dataReadError(),
                            currentFieldName, null).message("Unexpected token %s",
                            parser.getCurrentToken()).build(logger);
            }
        }
        list.endList();

    }
}
