package com.github.dangxia.footprint;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockSupportPermit {
	private static Logger logger = LoggerFactory
			.getLogger(LockSupportPermit.class);

	public static void info(String msg) {
		logger.info(msg);
	}

	public static class ParkThread extends Thread {
		@Override
		public void run() {
			info("park thread start ,then park");
			LockSupport.park();
			info("parked thread unpark");
		}

		public void unpark() {
			LockSupport.unpark(this);
		}
	}

	public static class DelayedParkThread extends ParkThread {
		@Override
		public void run() {
			info("park thread start,then sleep 1 second");
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			info("park thread excute park");
			LockSupport.park();
			info("parked thread unpark");
		}
	}

	/**
	 * <pre>
	 * [INFO ] 2014-05-04 23:10:55,770 park thread start ,then park
	 * [INFO ] 2014-05-04 23:10:55,770 main thread sleep 2 seconds,then unpark parked thread
	 * [INFO ] 2014-05-04 23:10:57,772 parked thread unpark
	 * </pre>
	 */
	public static class LockSupportParkBeforeUnpark {

		public static void main(String[] args) throws InterruptedException {
			ParkThread t = new ParkThread();
			t.start();
			TimeUnit.MILLISECONDS.sleep(100);
			info("main thread sleep 2 seconds,then unpark parked thread");
			TimeUnit.SECONDS.sleep(2);
			t.unpark();
		}
	}

	/**
	 * <pre>
	 * [INFO ] 2014-05-04 23:19:48,917 unpark thread
	 * [INFO ] 2014-05-04 23:19:48,917 park thread start,then sleep 1 second
	 * [INFO ] 2014-05-04 23:19:49,919 park thread excute park
	 * [INFO ] 2014-05-04 23:19:49,919 parked thread unpark
	 * </pre>
	 */
	public static class LockSupportUnparkBeforePark {
		public static void main(String[] args) {
			DelayedParkThread t = new DelayedParkThread();
			t.start();
			info("unpark thread");
			t.unpark();
		}
	}

	public static class LockSupportUnParksBeforePark {
		public static void main(String[] args) {
			DelayedParkThread t = new DelayedParkThread();
			t.start();
			info("unpark three times");
			t.unpark();
			t.unpark();
			t.unpark();
		}
	}
}
