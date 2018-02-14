package com.jtangney.gcpex.dataflow;

import java.io.Serializable;

import org.apache.avro.reflect.Nullable;

public class SimpleLogMessage implements Serializable {

	private static final long serialVersionUID = 1L;

	@Nullable
	private int httpStatusCode;
	@Nullable
	private String insertId;

	public SimpleLogMessage() {
	}

	public SimpleLogMessage(int httpStatusCode, String insertId) {
		this.httpStatusCode = httpStatusCode;
		this.insertId = insertId;
	}

	public int getHttpStatusCode() {
		return httpStatusCode;
	}

	public String getInsertId() {
		return insertId;
	}

	public String toString() {
		return String.format("%s:  %d", this.insertId, this.httpStatusCode);
	}
}
