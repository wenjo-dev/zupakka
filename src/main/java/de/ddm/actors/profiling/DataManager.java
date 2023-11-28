package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;

import java.util.ArrayList;

public abstract class DataManager {
    enum CurrentTask {
        ReadData,
        CreateColumns,
        CreateUniqueLists,
        FindUnaryINDS
    }
    static CurrentTask currentTask = CurrentTask.ReadData;
    static ArrayList<String[]>[] fileContents = new ArrayList[7];
    static int currentTableIndex = 0;
    static int currentColumnIndex = 0;

}

