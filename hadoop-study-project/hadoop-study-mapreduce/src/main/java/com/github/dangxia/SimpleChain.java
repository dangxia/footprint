package com.github.dangxia;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SimpleChain extends Configured implements Tool {

	private final JobControl jobControl;

	public SimpleChain() {
		this("simple-chain");
	}

	public SimpleChain(String jobname) {
		jobControl = new JobControl(jobname);
	}

	@Override
	public int run(String[] args) throws Exception {
		fillJobControl();
		new Thread(jobControl).start();
		return waitJobsComplete();
	}

	protected void fillJobControl() throws Exception {
		Job wordCountJob = SimpleChainMapper.createJob(newConf());
		Job sortJob = SimpleSort.createJob(newConf());

		ControlledJob cWordCountJob = new ControlledJob(
				wordCountJob.getConfiguration());
		cWordCountJob.setJob(wordCountJob);

		ControlledJob cSortJob = new ControlledJob(sortJob.getConfiguration());
		cSortJob.setJob(sortJob);
		cSortJob.addDependingJob(cWordCountJob);

		jobControl.addJob(cWordCountJob);
		jobControl.addJob(cSortJob);
	}

	private int waitJobsComplete() throws Exception {
		try {
			while (!jobControl.allFinished()) {
				if (jobControl.getFailedJobList().size() > 0) {
					printJobsState();
					return 1;
				}
				printJobsState();
				TimeUnit.SECONDS.sleep(1);
			}
			printJobsState();
			if (jobControl.getFailedJobList().size() > 0) {
				return 1;
			}
			return 0;
		} catch (Exception e) {
			throw e;
		} finally {
			jobControl.stop();
		}

	}

	private void printJobsState() {
		List<ControlledJob> jobs = jobControl.getReadyJobsList();
		System.out.print("-------ready jobs size:" + jobs.size() + ",");
		for (ControlledJob job : jobs) {
			System.out.print(job.getJobName() + "\t");
		}
		System.out.println("");

		jobs = jobControl.getWaitingJobList();
		System.out.print("-------waiting jobs size:" + jobs.size() + ",");
		for (ControlledJob job : jobs) {
			System.out.print(job.getJobName() + "\t");
		}
		System.out.println("");

		jobs = jobControl.getRunningJobList();
		System.out.print("-------running jobs size:" + jobs.size() + ",");
		for (ControlledJob job : jobs) {
			System.out.print(job.getJobName() + "\t");
		}
		System.out.println("");

		jobs = jobControl.getSuccessfulJobList();
		System.out.print("-------Successed jobs size:" + jobs.size() + ",");
		for (ControlledJob job : jobs) {
			System.out.print(job.getJobName() + "\t");
		}
		System.out.println("");

		jobs = jobControl.getFailedJobList();
		System.out.print("-------Failed jobs size:" + jobs.size() + ",");
		for (ControlledJob job : jobs) {
			System.out.print(job.getJobName() + "\t");
		}
		System.out.println("");
	}

	protected Configuration newConf() {
		Configuration conf = new Configuration();
		Configuration base = getConf();
		for (Entry<String, String> entry : base) {
			base.set(entry.getKey(), entry.getValue());
		}
		return conf;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SimpleChain(), args));
	}

	public JobControl getJobControl() {
		return jobControl;
	}

}
