package de.ddm.actors.profiling.tasks;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;

@NoArgsConstructor
@Getter
public class INDTask extends WorkTask {
    ArrayList<String> c1;
    ArrayList<String> c2;
    int c1TableIndex;
    int c1ColumnIndex;
    int c2TableIndex;
    int c2ColumnIndex;

    public INDTask(ArrayList<String> c1, ArrayList<String> c2, int c1TableIndex, int c1ColumnIndex, int c2TableIndex, int c2ColumnIndex){
        this.c1 = c1;
        this.c2 = c2;
        this.c1TableIndex = c1TableIndex;
        this.c1ColumnIndex = c1ColumnIndex;
        this.c2TableIndex = c2TableIndex;
        this.c2ColumnIndex = c2ColumnIndex;
        this.id = WorkTask.stId++;
    }
}
