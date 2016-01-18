package com.coyotesong.coursera.cloud.hadoop;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * This is a simple CSV parser using a FSM model.
 * 
 * @author bgiles
 */
public class CSVParser {
    private enum State {
        START, QUOTED, ESCAPED
    };

    public static List<String> parse(String s) {
        final List<String> results = new ArrayList<>();
        final Stack<State> state = new Stack<>();
        final StringBuilder sb = new StringBuilder();

        state.push(State.START);
        for (char ch : s.toCharArray()) {
            switch (state.peek()) {
            case START:
                if (ch == ',') {
                    results.add(sb.toString());
                    sb.setLength(0);
                } else if (ch == '\"') {
                    state.push(State.QUOTED);
                } else if (ch == '\\') {
                    state.push(State.ESCAPED);
                } else {
                    sb.append(ch);
                }
                break;
            case QUOTED:
                if (ch == '\"') {
                    state.pop();
                } else if (ch == '\\') {
                    state.push(State.ESCAPED);
                } else {
                    sb.append(ch);
                }
                break;
            case ESCAPED:
                sb.append(ch);
                state.pop();
                break;
            }
        }

        assert state.size() == 1 && state.peek() == State.START;

        return results;
    }
}
