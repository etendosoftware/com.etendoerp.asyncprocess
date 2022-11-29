package com.etendoerp.asyncprocess.util;

import com.smf.jobs.model.Job;
import com.smf.jobs.model.JobLine;

public class TopicUtil {

  public static String createTopic(Job job, JobLine jobLine) {
    return createTopic(job, jobLine, null);
  }

  public static String createTopic(Job job, String subTopic) {
    return createTopic(job, null, subTopic);
  }

  public static String createTopic(Job job, JobLine jobLine, String subtopic) {
    return job.getName().replace(" ", "_") +
        "." +
        (jobLine != null ? jobLine.getLineNo() : subtopic);
  }

}
