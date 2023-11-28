package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;

import java.util.ArrayList;

public abstract class DataManager {
    enum CurrentTask {
        CreateUniqueLists,
        FindUnaryINDS
    }
    static CurrentTask currentTask = CurrentTask.CreateUniqueLists;
    static int currentTableIndex = 0;
    static int currentColumnIndex = 0;
    static boolean workAvailable = false;
    static ArrayList<String[]>[] fileContents = new ArrayList[7];
    static ArrayList<ActorRef<DependencyWorker.Message>> availableWorkers = new ArrayList<akka.actor.typed.ActorRef<DependencyWorker.Message>>();

}

