package de.ddm.actors.profiling.tasks;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class UniqueColumnTask extends WorkTask {
    String[] data;
    int tableIndex;
    int columnIndex;
}
