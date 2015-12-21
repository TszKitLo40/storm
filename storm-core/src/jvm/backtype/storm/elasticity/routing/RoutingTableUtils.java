package backtype.storm.elasticity.routing;

/**
 * Created by robert on 12/18/15.
 */
public class RoutingTableUtils {

    public static BalancedHashRouting getBalancecHashRouting(RoutingTable routingTable) {
        if(routingTable instanceof BalancedHashRouting) {
            return (BalancedHashRouting)routingTable;
        } else if ((routingTable instanceof PartialHashingRouting) && (((PartialHashingRouting) routingTable).getOriginalRoutingTable() instanceof BalancedHashRouting)) {
            return (BalancedHashRouting)((PartialHashingRouting) routingTable).getOriginalRoutingTable();
        } else
            return null;
    }
}
