package de.ddm.actors.profiling.tasks;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class INDTask extends WorkTask {
    ArrayList<String> c1;
    ArrayList<String> c2;
    int c1TableIndex;
    int c1ColumnIndex;
    int c2TableIndex;
    int c2ColumnIndex;
}
