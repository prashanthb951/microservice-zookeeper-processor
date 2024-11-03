package com.example.zookepertest.demo.service;

import java.util.List;

public interface TaskService {
   void processTasks(List<String> assignedTaskList);
   void stopTaskProcessing();
}
