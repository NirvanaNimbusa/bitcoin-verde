package com.softwareverde.bitcoin.transaction.script;

import com.softwareverde.bitcoin.transaction.script.locking.LockingScript;
import com.softwareverde.bitcoin.transaction.script.opcode.Operation;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.json.Json;
import com.softwareverde.util.bytearray.ByteArrayBuilder;

public class ScriptDeflater {
    public MutableByteArray toBytes(final Script script) {
        final ByteArrayBuilder byteArrayBuilder = new ByteArrayBuilder();
        for (final Operation operation : script.getOperations()) {
            byteArrayBuilder.appendBytes(operation.getBytes());
        }
        return MutableByteArray.wrap(byteArrayBuilder.build());
    }

    public String toString(final Script script) {
        final List<Operation> scriptOperations = script.getOperations();
        if (scriptOperations == null) { return null; }

        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < scriptOperations.getCount(); ++i) {
            final Operation operation = scriptOperations.get(i);
            stringBuilder.append("(");
            stringBuilder.append(operation.toString());
            stringBuilder.append(")");

            if (i + 1 < scriptOperations.getCount()) {
                stringBuilder.append(" ");
            }
        }
        return stringBuilder.toString();
    }

    /**
     * Serializes the script in a format the is commonly used by other applications.
     *  This format is a more terse version of ScriptDeflater.toString(); Operations are merely their hex opcodes.
     */
    public String toStandardString(final Script script) {
        final List<Operation> scriptOperations = script.getOperations();
        if (scriptOperations == null) { return null; }

        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < scriptOperations.getCount(); ++i) {
            final Operation operation = scriptOperations.get(i);
            stringBuilder.append(operation.toStandardString());

            if (i + 1 < scriptOperations.getCount()) {
                stringBuilder.append(" ");
            }
        }
        return stringBuilder.toString();
    }

    public Json toJson(final Script script) {
        final ByteArray scriptByteArray = script.getBytes();

        final Json json = new Json();
        json.put("bytes", scriptByteArray);

        final ScriptType scriptType;
        {
            if (script instanceof LockingScript) {
                final ScriptPatternMatcher scriptPatternMatcher = new ScriptPatternMatcher();
                scriptType = scriptPatternMatcher.getScriptType((LockingScript) script);
            }
            else {
                scriptType = ScriptType.CUSTOM_SCRIPT;
            }
        }
        json.put("scriptType", scriptType);

        final Json operationsJson;
        final List<Operation> operations = script.getOperations();
        if (operations != null) {
            operationsJson = new Json();
            for (final Operation operation : operations) {
                operationsJson.add(operation);
            }
        }
        else {
            operationsJson = null;
        }
        json.put("operations", operationsJson);

        return json;
    }
}
