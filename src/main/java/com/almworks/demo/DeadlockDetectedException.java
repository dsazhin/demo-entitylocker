package com.almworks.demo;

public class DeadlockDetectedException extends RuntimeException {
	public DeadlockDetectedException(String msg) {
		super(msg);
	}
}
