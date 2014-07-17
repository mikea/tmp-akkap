import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.persistence.AbstractPersistentActor;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import static akka.actor.ActorRef.noSender;

/**
 * @author mike.aizatsky@gmail.com
 */
public class PersistencePerformanceTest {
    public static class TestActor extends AbstractPersistentActor {
        private long startTime = -1;
        private long messages = 0;

        @Override
        public PartialFunction<Object, BoxedUnit> receiveRecover() {
            return ReceiveBuilder.matchAny(msg -> { }).build();
        }

        @Override
        public PartialFunction<Object, BoxedUnit> receiveCommand() {
            return ReceiveBuilder
                    .match(Integer.class, i -> persist(i, msg -> {
                        messages++;
                        if (startTime < 0) {
                            startTime = System.currentTimeMillis();
                        }
                    }))
                    .matchEquals("DONE", evt -> {
                        System.out.println(String.format("Received %d messages in %d ms", messages, System.currentTimeMillis() - startTime));
                    })
                    .build();
        }

        @Override
        public String persistenceId() {
            return "test";
        }
    }


    public static void main(String[] args) {
        ActorSystem actorSystem = ActorSystem.create();

        ActorRef ref = actorSystem.actorOf(Props.create(TestActor.class));

        System.out.println("Sending messages...");
        for (int i = 0; i < 100_000; ++i) {
            ref.tell(i, noSender());
        }

        System.out.println("Done sending messages");
        ref.tell("DONE", noSender());

    }
}
