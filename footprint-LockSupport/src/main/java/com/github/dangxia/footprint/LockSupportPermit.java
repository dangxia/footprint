package com.github.dangxia.footprint;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class LockSupportPermit {
	public static void main(String[] args) throws InterruptedException {
		Thread thread = new Thread() {
			public void run() {
				System.out.println("thread start");
				LockSupport.park();
				System.out.println("thread unpark");
			};
		};
		thread.start();
		LockSupport.unpark(thread);
		LockSupport.unpark(thread);
		LockSupport.unpark(thread);
		System.out.println("main thread sleep 2 second");
		TimeUnit.SECONDS.sleep(2);
		System.out.println("main thread unpark unparked thread");
		LockSupport.unpark(thread);
	}
}
