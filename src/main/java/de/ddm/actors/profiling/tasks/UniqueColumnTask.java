package de.ddm.actors.profiling.tasks;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;

@AllArgsConstructor
@Getter
public class UniqueColumnTask extends WorkTask {
    String[] data;
    int tableIndex;
    int columnIndex;

    public static void createUniqueColumnTasks(ArrayList<String[]> tableData, int tableIndex){
        for (int i = 0; i < tableData.get(0).length; i++){
            String[] column = new String[tableData.size()];
            for (int j = 0; j < tableData.size(); j++){
                column[j] = tableData.get(j)[i];
            }
            DataManager.workTasks.add(new UniqueColumnTask(column, tableIndex, i));
        }
    }
}

