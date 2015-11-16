package backtype.storm.elasticity.routing;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Robert on 11/12/15.
 */
public class PartialHashingRouting extends HashingRouting {


    /* the set of valid routes in this routing table */
    Set<Integer> _validRoutes = new HashSet<>();
    /**
     * @param nRoutes is the number of routes processed by elastic tasks.
     */
    public PartialHashingRouting(int nRoutes) {
        super(nRoutes);
        for(int i = 0; i < nRoutes; i++) {
            _validRoutes.add(i);
        }
    }

    public PartialHashingRouting(HashingRouting hashingRouting) {
        super(hashingRouting);
        _validRoutes.addAll(super.getRoutes());
    }

    public PartialHashingRouting setExceptionRoutes(ArrayList<Integer> exceptionRoutes) {
        _validRoutes.addAll(getRoutes());
        _validRoutes.removeAll(exceptionRoutes);
        return this;
    }

    public PartialHashingRouting addExceptionRoutes(ArrayList<Integer> exceptionRoutes) {
        _validRoutes.removeAll(exceptionRoutes);
        return this;
    }

    public PartialHashingRouting addExceptionRoute(Integer exception) {
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

    @Override
    public int route(Object key) {
        int route = super.route(key);
        if (route == RoutingTable.origin || _validRoutes.contains(route))
            return route;
        else
            return RoutingTable.remote;
    }

    public int getOrignalRoute(Object key) {
        return super.route(key);
    }

    public PartialHashingRouting createComplementRouting() {
        PartialHashingRouting ret = new PartialHashingRouting(this);
        ret._validRoutes.removeAll(this._validRoutes);
        return ret;
    }

    public void invalidAllRoutes() {
        _validRoutes.clear();
    }

    public void addValidRoute(int route) {
        ArrayList<Integer> list = new ArrayList<>();
        list.add(route);
        addValidRoutes(list);
    }

    public void addValidRoutes(ArrayList<Integer> routes) {
        for(int i: routes) {
            if(super.getRoutes().contains(i)) {
                _validRoutes.add(i);
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("hello world");
        ArrayList<Integer> exceptions = new ArrayList<>();
        exceptions.add(1);
        exceptions.add(15);
        exceptions.add(4);

        RoutingTable routingTable = new PartialHashingRouting(10);
        System.out.println("Routes:"+routingTable.getRoutes());

        ((PartialHashingRouting)routingTable).setExceptionRoutes(exceptions);

        System.out.println("100:"+routingTable.route(100));
        System.out.println("300:"+routingTable.route(300));
        System.out.println("Routes:"+routingTable.getRoutes());

        ((PartialHashingRouting)routingTable).addExceptionRoute(3);
        ((PartialHashingRouting)routingTable).addExceptionRoute(1000);
        ((PartialHashingRouting)routingTable).addExceptionRoute(7);
        System.out.println("Routes:"+routingTable.getRoutes());





    }


}
