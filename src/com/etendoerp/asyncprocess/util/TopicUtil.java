package com.etendoerp.asyncprocess.util;

import com.smf.jobs.model.Job;

public class TopicUtil {
  private TopicUtil() {
  }

  public static String createTopic(Job job, Long subtopic) {
    return createTopic(job, Long.toString(subtopic));
  }

  public static String createTopic(Job job, String subtopic) {
    return job.getName().replace(" ", "_") + "." + subtopic;
  }

}
