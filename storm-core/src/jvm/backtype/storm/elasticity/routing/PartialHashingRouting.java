package backtype.storm.elasticity.routing;

import backtype.storm.elasticity.utils.Histograms;

import java.util.*;

/**
 * Created by Robert on 11/12/15.
 */
public class PartialHashingRouting implements RoutingTable {


    /* the set of valid routes balls this routing table */
    Set<Integer> _validRoutes = new HashSet<>();

    RoutingTable _routingTable;

//    /**
//     * @param nRoutes is the number of routes processed by elastic tasks.
//     */
//    public PartialHashingRouting(int nRoutes) {
//        super(nRoutes);
//        for(int i = 0; i < nRoutes; i++) {
//            _validRoutes.add(i);
//        }
//    }

    public PartialHashingRouting(RoutingTable hashingRouting) {
        _routingTable = hashingRouting;
//        super(hashingRouting);
        _validRoutes.addAll(_routingTable.getRoutes());
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
    public Histograms getRoutingDistribution() {
        return _routingTable.getRoutingDistribution();
    }

    @Override
    public synchronized void enableRoutingDistributionSampling() {
        _routingTable.enableRoutingDistributionSampling();
    }

    @Override
    public synchronized int scalingOut() {
        int newRouteIndex = -1;
//        if(_routingTable instanceof BalancedHashRouting) {
            newRouteIndex = _routingTable.scalingOut();
//        }
        _validRoutes.add(newRouteIndex);
        return newRouteIndex;
    }

    @Override
    public synchronized void scalingIn() {
        _routingTable.scalingIn();
        _validRoutes.remove(_routingTable.getNumberOfRoutes());
    }

    @Override
    public synchronized int route(Object key) {
        int route = _routingTable.route(key);
        if (_validRoutes.contains(route))
            return route;
        else
            return RoutingTable.remote;
    }

    public List<Integer> getOriginalRoutes() {
        return _routingTable.getRoutes();
    }

    public List<Integer> getExceptionRoutes() {
        List<Integer> ret = getOriginalRoutes();
        ret.removeAll(getRoutes());
        return ret;
    }

    public int getOrignalRoute(Object key) {
        return _routingTable.route(key);
    }

    public PartialHashingRouting createComplementRouting() {
        PartialHashingRouting ret = new PartialHashingRouting(_routingTable);
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

    public synchronized void addValidRoutes(List<Integer> routes) {
        for(int i: routes) {
            if(_routingTable.getRoutes().contains(i)) {
                _validRoutes.add(i);
            } else {
                System.out.println("Cannot added routes "+i+", because it is not a valid route");
            }
        }
    }

    public String toString() {
        String ret = "PartialHashRouting: \n";
        ret += "number of original routes: " + getOriginalRoutes() + "\n";
        ret += "number of valid routes: " + getNumberOfRoutes() + "\n";
        ret += "valid routes: " + _validRoutes + "\n";
        ret += this._routingTable.toString();
        return ret;
    }

    public RoutingTable getOriginalRoutingTable() {
        return _routingTable;
    }

    public static void main(String[] args) {

        HashingRouting routing = new HashingRouting(3);

        PartialHashingRouting partialHashingRouting = new PartialHashingRouting(routing);

        PartialHashingRouting complement = partialHashingRouting.addExceptionRoute(2).createComplementRouting();

        System.out.println("Complement:"+complement.getRoutes());


        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        map.put(0,0);
        map.put(1,0);
        map.put(2,1);
        map.put(3,1);
        BalancedHashRouting balancedHashRouting = new BalancedHashRouting(map,2);

        PartialHashingRouting partialHashingRouting2 = new PartialHashingRouting(balancedHashRouting);

        System.out.println("2-->" + partialHashingRouting2.route(1));

        PartialHashingRouting complement2 = partialHashingRouting2.addExceptionRoute(1);

        System.out.println("2-->" + partialHashingRouting2.route(1));

        System.out.println("Complement:"+complement2.getRoutes());





    }


}
