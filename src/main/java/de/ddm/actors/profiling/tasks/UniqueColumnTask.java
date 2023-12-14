package de.ddm.actors.profiling.tasks;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;

@NoArgsConstructor
@Getter
public class UniqueColumnTask extends WorkTask {
    ArrayList<String> data;
    int tableIndex;
    int columnIndex;

    public UniqueColumnTask(ArrayList<String> data, int tableIndex, int columnIndex){
        this.data = data;
        this.tableIndex = tableIndex;
        this.columnIndex = columnIndex;
        this.id = WorkTask.stId++;
    }
}
