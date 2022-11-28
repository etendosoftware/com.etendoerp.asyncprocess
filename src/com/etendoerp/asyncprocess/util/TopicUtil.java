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
    String topic;
    topic = job.getName().replaceAll(" ", "_") + ".";
    if (jobLine != null) {
      topic += jobLine.getLineNo();
    } else {
      topic += subtopic;
    }
    return topic;
  }

}
