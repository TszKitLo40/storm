package backtype.storm.elasticity.routing;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

/**
 * Created by Robert on 11/12/15.
 */
public class ParitalHashingRouting extends HashingRouting {


    /* the set of valid routes in this routing table */
    Set<Integer> _validRoutes = new HashSet<>();
    /**
     * @param nRoutes is the number of routes processed by elastic tasks.
     */
    public ParitalHashingRouting(int nRoutes) {
        super(nRoutes);
        for(int i = 0; i < nRoutes; i++) {
            _validRoutes.add(i);
        }
    }

    public ParitalHashingRouting(HashingRouting hashingRouting) {
        super(hashingRouting);
        _validRoutes.addAll(getRoutes());
    }

    public ParitalHashingRouting setExceptionRoutes(ArrayList<Integer> exceptionRoutes) {
        _validRoutes.addAll(getRoutes());
        _validRoutes.removeAll(exceptionRoutes);
        return this;
    }

    public ParitalHashingRouting addExceptionRoutes(ArrayList<Integer> exceptionRoutes) {
        _validRoutes.removeAll(exceptionRoutes);
        return this;
    }

    public ParitalHashingRouting addExceptionRoute(Integer exception) {
        _validRoutes.remove(exception);
        return this;
    }

    @Override
    public int getNumberOfRoutes() {
        return _validRoutes.size();
    }


    @Override
    public ArrayList<Integer> getRoutes() {
        return new ArrayList<>(_validRoutes);
    }
    public static void main(String[] args) {
        System.out.println("hello world");
        ArrayList<Integer> exceptions = new ArrayList<>();
        exceptions.add(1);
        exceptions.add(15);
        exceptions.add(4);

        RoutingTable routingTable = new ParitalHashingRouting(10);
        System.out.println("Routes:"+routingTable.getRoutes());

        ((ParitalHashingRouting)routingTable).setExceptionRoutes(exceptions);

        System.out.println("100:"+routingTable.route(100));
        System.out.println("300:"+routingTable.route(300));
        System.out.println("Routes:"+routingTable.getRoutes());

        ((ParitalHashingRouting)routingTable).addExceptionRoute(3);
        ((ParitalHashingRouting)routingTable).addExceptionRoute(1000);
        ((ParitalHashingRouting)routingTable).addExceptionRoute(7);
        System.out.println("Routes:"+routingTable.getRoutes());





    }


}
