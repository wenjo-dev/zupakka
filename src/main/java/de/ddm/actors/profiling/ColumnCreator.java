package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.profiling.tasks.UniqueColumnTask;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;

public class ColumnCreator extends AbstractBehavior<ColumnCreator.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CreateColumnsMessage implements Message {
        private static final long serialVersionUID = 1739062314525633711L;
        ActorRef<DependencyMiner.Message> replyTo;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "columnCreator";

    public static Behavior<Message> create(final int id, final ArrayList<String[]> table) {
        return Behaviors.setup(context -> new ColumnCreator(context, id, table));
    }

    private ColumnCreator(ActorContext<Message> context, final int id, final ArrayList<String[]> table) {
        super(context);
        this.id = id;
        this.table = table;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final int id;
    private final ArrayList<String[]> table;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(CreateColumnsMessage.class, this::handle)
                .build();
    }

    private Behavior<ColumnCreator.Message> handle(ColumnCreator.CreateColumnsMessage message) {
        ArrayList<UniqueColumnTask> taskList = new ArrayList<>();
        for (int i = 0; i < this.table.get(0).length; i++){
            String[] column = new String[this.table.size()];
            for (int j = 0; j < this.table.size(); j++){
                column[j] = this.table.get(j)[i];
            }
            taskList.add(new UniqueColumnTask(column, this.id, i));
        }
        message.getReplyTo().tell(new DependencyMiner.ColumnCreationMessage(this.getContext().getSelf(), taskList));
        return this;
    }

}
