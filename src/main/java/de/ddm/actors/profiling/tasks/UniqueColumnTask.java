package de.ddm.actors.profiling.tasks;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class UniqueColumnTask extends WorkTask {
    ArrayList<String> data;
    int tableIndex;
    int columnIndex;
}
