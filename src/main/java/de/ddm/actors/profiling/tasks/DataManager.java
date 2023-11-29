package de.ddm.actors.profiling.tasks;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;

public abstract class DataManager {
    public enum CurrentTask {
        ReadData,
        RemoveDuplicates,
        FindUnaryINDS
    }
    public static CurrentTask currentTask = CurrentTask.ReadData;
    public static ArrayList<String[]>[] fileContents = new ArrayList[7];
    public static ArrayList<WorkTask> workTasks = new ArrayList<>();
}


