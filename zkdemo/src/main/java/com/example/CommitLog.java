package com.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.ArrayList;

public class CommitLog {
    private String logFile;
    private int sequence=-1;

    public CommitLog(String logFile) {
        this.logFile = logFile;
    }

    public void log(String msg) {
        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)))) {
            out.println(msg);
            sequence += 1;
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    public List<String> readLines(int startSeq) {
        BufferedReader reader;
        List<String> output = new ArrayList<String>();

		try {
            int s = 0;
			reader = new BufferedReader(new FileReader(logFile));
            while(true) {
                String line = reader.readLine();
                if (line == null) break;
                if (s >= startSeq) {
                    output.add(line);
                }
                s += 1;
            }
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

        return output;
    }

    public int getSequence() {
        return sequence;
    }
}
