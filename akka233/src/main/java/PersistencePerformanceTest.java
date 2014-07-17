import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.persistence.AbstractProcessor;
import akka.persistence.Persistent;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import static akka.actor.ActorRef.noSender;

/**
 * @author mike.aizatsky@gmail.com
 */
public class PersistencePerformanceTest {
    public static class TestActor extends AbstractProcessor {
        private long startTime = -1;
        private long messages = 0;

        @Override
        public PartialFunction<Object, BoxedUnit> receive() {
            return ReceiveBuilder
                    .match(Persistent.class, p -> {
                        messages++;
                        if (startTime < 0) {
                            startTime = System.currentTimeMillis();
                        }
                    })
                    .matchEquals("DONE", evt -> {
                        System.out.println(String.format("Received %d messages in %d ms", messages, System.currentTimeMillis() - startTime));
                    })
                    .build();
        }
    }


    public static void main(String[] args) {
        ActorSystem actorSystem = ActorSystem.create();

        ActorRef ref = actorSystem.actorOf(Props.create(TestActor.class));

        System.out.println("Sending messages...");
        for (int i = 0; i < 100_000; ++i) {
            ref.tell(Persistent.create(i), noSender());
        }

        System.out.println("Done sending messages");
        ref.tell("DONE", noSender());

    }
}
