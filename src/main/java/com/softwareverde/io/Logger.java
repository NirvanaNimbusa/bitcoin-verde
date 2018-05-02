package com.softwareverde.io;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Logger {
    public interface LogCallback {
        void onLog(Object message);
    }

    private static final Integer MAX_BATCH_COUNT = 128;
    private static final ConcurrentLinkedQueue<String> _queuedMessages = new ConcurrentLinkedQueue<String>();
    private static final Thread _logThread = new Thread(new Runnable() {
        private String _dequeueMessages(final Integer maxMessageCount) {
            final StringBuilder stringBuilder = new StringBuilder();
            int messageCount = 0;
            String message;
            while ((message = _queuedMessages.poll()) != null) {
                stringBuilder.append(message);
                stringBuilder.append("\n");

                messageCount += 1;
                if (messageCount >= maxMessageCount) { break; }
            }

            return stringBuilder.toString();
        }

        @Override
        public void run() {
            while (true) {
                try { Thread.sleep(500); } catch (final Exception exception) { break; }

                final String concatenatedMessages = _dequeueMessages(MAX_BATCH_COUNT);
                if (! concatenatedMessages.isEmpty()) {
                    System.out.print(concatenatedMessages);
                    System.out.flush();
                }
            }

            String message;
            while ((message = _queuedMessages.poll()) != null) {
                System.out.println(message);
            }
        }
    });
    static {
        _logThread.start();
    }

    public static LogCallback LOG_CALLBACK = null;

    protected static String _getMetadata(final Exception exception, final Integer backtraceIndex) {
        final StackTraceElement stackTraceElements[] = exception.getStackTrace();
        final StackTraceElement stackTraceElement = stackTraceElements[backtraceIndex];
        return stackTraceElement.getFileName() + ":" + stackTraceElement.getLineNumber();
    }

    public static void log(final Object object) {
        try (final StringWriter stringWriter = new StringWriter()) {
            try (final PrintWriter printWriter = new PrintWriter(stringWriter)) {
                if (object instanceof Exception) {
                    final String metadata = ("[" + _getMetadata((Exception) object, 0) + "]");
                    stringWriter.append(metadata);
                    ((Exception) object).printStackTrace(printWriter);
                }
                else {
                    final String metadata = ("[" + _getMetadata(new Exception(), 1) + "] ");
                    stringWriter.append(metadata);
                    stringWriter.append(object.toString());
                }

                _queuedMessages.add(stringWriter.toString());
            }
        }
        catch (final Exception exception) { exception.printStackTrace(); }

        final LogCallback logCallback = LOG_CALLBACK;
        if (logCallback != null) {
            logCallback.onLog(object);
        }
    }
}
