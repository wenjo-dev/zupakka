package de.ddm.actors.profiling.tasks;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class UniqueColumnTask extends WorkTask {
    String[] data;
    int tableIndex;
    int columnIndex;
}
