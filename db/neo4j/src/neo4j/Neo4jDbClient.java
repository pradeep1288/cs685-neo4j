package neo4j;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.ObjectByteIterator;
import edu.usc.bg.base.StringByteIterator;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: pradeepnayak
 * Date: 19/11/13
 * Time: 10:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class Neo4jDbClient extends DB {
    private static final String DB_PATH = "/Users/pradeepnayak/Documents/neo4j-community-2.0.0-RC1/data/graph.db";
    static GraphDatabaseService graphDb;

    public boolean init() {
        System.out.println("Starting neo4j.....\n");
        if (graphDb == null) {
            graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
        }
        System.out.println("Created graphdb object");
        return true;
    }

    public void cleanup(boolean warmup) {
        System.out.println("Exiting neo4j.....\n");
        //graphDb.shutdown();
    }

    @Override
    public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values, boolean insertImage) {
        //System.out.println("Inside of Insert Entity");
        Label userLabel = DynamicLabel.label("user");
        if (entitySet.equals("users")) {
            //System.out.println("Inserting user");
            Node myTempNode;
            Transaction tx = graphDb.beginTx();
            myTempNode = graphDb.createNode();
            myTempNode.addLabel(userLabel);
            myTempNode.setProperty("userid", entityPK);
            try {
                for (String k : values.keySet()) {
                    if (!(k.toString().equalsIgnoreCase("pic") || k.toString().equalsIgnoreCase("tpic")))
                        myTempNode.setProperty(k, values.get(k).toString());
                }
                tx.success();
            } finally {
                tx.close();
            }

        }
        if (entitySet.equals("resources")) {
            // System.out.println("Adding resources..");
            Node myTempResourceNode;
            Transaction tx = graphDb.beginTx();
            myTempResourceNode = graphDb.createNode();
            myTempResourceNode.addLabel(DynamicLabel.label("resource"));
            try {
                for (String k : values.keySet()) {
                    if (!(k.toString().equalsIgnoreCase("pic") || k.toString().equalsIgnoreCase("tpic")))
                        myTempResourceNode.setProperty(k, values.get(k).toString());
                }


                String wallUserId = values.get("walluserid").toString();
                try {
                    ResourceIterator<Node> users = graphDb.findNodesByLabelAndProperty(userLabel, "userid", wallUserId).iterator();
                    ArrayList<Node> userNodes = new ArrayList<Node>();
                    while (users.hasNext()) {
                        userNodes.add(users.next());
                    }

                    for (Node node : userNodes) {
                        //System.out.println("hey there is some node which has a relation");
                        node.createRelationshipTo(myTempResourceNode, RelTypes.OWNS);
                    }
                } finally {

                }

                tx.success();
            } finally {
                tx.close();
            }

        }
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private static enum RelTypes implements RelationshipType {
        FRIEND, OWNS, PENDING_FRIEND, CREATED
    }

    @Override
    public int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode) {
        Transaction tx = graphDb.beginTx();
        try {
            Node myTempNode = findNodeByUserid(profileOwnerID + "");
            if (requesterID == profileOwnerID)
                result.put("pendingcount", new ObjectByteIterator(Integer.toString(countRelationships(myTempNode, RelTypes.PENDING_FRIEND, Direction.INCOMING)).getBytes()));
            result.put("friendcount", new ObjectByteIterator(Integer.toString(countRelationships(myTempNode, RelTypes.FRIEND, null)).getBytes()));
            result.put("resourcecount", new ObjectByteIterator(Integer.toString(countRelationships(myTempNode, RelTypes.OWNS, null)).getBytes()));
            addPropertiesToMap(result, myTempNode);
            tx.success();
        } finally {
            tx.close();
        }
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private void addPropertiesToMap(HashMap<String, ByteIterator> result, Node myTempNode) {
        for (String property : myTempNode.getPropertyKeys()) {
            result.put(property, new StringByteIterator(myTempNode.getProperty(property).toString()));
        }
    }

    private int countRelationships(Node myTempNode, RelTypes relType, Direction direction) {
        int pendingFriendsCount = 0;
        Iterator<Relationship> pendingFriendsIterator;
        if (direction != null) pendingFriendsIterator = myTempNode.getRelationships(relType, direction).iterator();
        else pendingFriendsIterator = myTempNode.getRelationships(relType).iterator();
        while (pendingFriendsIterator.hasNext()) {
            pendingFriendsCount++;

            pendingFriendsIterator.next();
        }
        return pendingFriendsCount;
    }

    @Override
    public int listFriends(int requesterID, int profileOwnerID, Set<String> fields, Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {
        Transaction tx = graphDb.beginTx();
        try {
            listFriendProperties(profileOwnerID, result, RelTypes.FRIEND, null);
            tx.success();
        }
        finally {
            tx.close();
        }
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private void listFriendProperties(int profileOwnerID, Vector<HashMap<String, ByteIterator>> result, RelTypes relType, Direction direction) {
        Node myTempNode = findNodeByUserid(profileOwnerID + "");
        Iterable<Relationship> iterator;
        if(direction==null) iterator = myTempNode.getRelationships(relType);
        else iterator = myTempNode.getRelationships(relType, direction);
        for (Relationship rel : iterator) {
            Node friend = rel.getOtherNode(myTempNode);
            HashMap<String, ByteIterator> friendProperties = new HashMap<String, ByteIterator>();
            addPropertiesToMap(friendProperties, friend);
            result.add(friendProperties);
        }
    }

    @Override
    public int viewFriendReq(int profileOwnerID, Vector<HashMap<String, ByteIterator>> results, boolean insertImage, boolean testMode) {
        listFriendProperties(profileOwnerID, results, RelTypes.PENDING_FRIEND, Direction.INCOMING);
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int acceptFriend(int inviterID, int inviteeID) {

        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int rejectFriend(int inviterID, int inviteeID) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int inviteFriend(int inviterID, int inviteeID) {
        addRelationship(inviteeID, inviteeID, RelTypes.PENDING_FRIEND);
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int viewTopKResources(int requesterID, int profileOwnerID, int k, Vector<HashMap<String, ByteIterator>> result) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getCreatedResources(int creatorID, Vector<HashMap<String, ByteIterator>> result) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int viewCommentOnResource(int requesterID, int profileOwnerID, int resourceID, Vector<HashMap<String, ByteIterator>> result) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int postCommentOnResource(int commentCreatorID, int resourceCreatorID, int resourceID, HashMap<String, ByteIterator> values) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int delCommentOnResource(int resourceCreatorID, int resourceID, int manipulationID) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int thawFriendship(int friendid1, int friendid2) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public HashMap<String, String> getInitialStats() {
        HashMap<String, String> stats = new HashMap<String, String>();
        Iterator<Node> iterUser;
        Transaction tx = graphDb.beginTx();
        int usercount = 0;
        int friendcount = 0;
        int pendingFriends = 0;
        int noOfResources = 0;
        try {
            GlobalGraphOperations gObj = GlobalGraphOperations.at(graphDb);
            iterUser = gObj.getAllNodesWithLabel(DynamicLabel.label("user")).iterator();
            while (iterUser.hasNext()) {
                Node n = iterUser.next();
                //System.out.println(n.getProperty("username"));
                friendcount = getRelationshipCount(n,RelTypes.FRIEND,null);
                pendingFriends = getRelationshipCount(n, RelTypes.PENDING_FRIEND, Direction.INCOMING);
                noOfResources = getRelationshipCount(n, RelTypes.OWNS, Direction.OUTGOING);
                usercount++;
            }
            tx.success();
        } finally {
            tx.close();
        }
        //System.out.println("User count is: " + usercount);
        stats.put("usercount", usercount + "");
        stats.put("avgfriendsperuser", (friendcount / usercount) + "");
        stats.put("avgpendingperuser", (pendingFriends/ usercount) + "");
        stats.put("resourcesperuser", (noOfResources/usercount) + "");
        return stats;
    }

    private int getRelationshipCount(Node n, RelTypes type, Direction direction) {
        int count = 0 ;
        Iterator<Relationship> iterFriendRel;
        if (direction == null)
            iterFriendRel = n.getRelationships(type).iterator();
        else
            iterFriendRel = n.getRelationships(type, direction).iterator();
        while (iterFriendRel.hasNext()) {
            count++;
            iterFriendRel.next();
        }
        return count;
    }

    @Override
    public int CreateFriendship(int friendid1, int friendid2) {
        addRelationship(friendid1, friendid2, RelTypes.FRIEND);
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private void addRelationship(int from, int to, RelTypes relType) {
        Label userLabel = DynamicLabel.label("user");
        ArrayList<Node> userNodes = new ArrayList<Node>();
        Transaction tx = graphDb.beginTx();
        Node myTempNode1 = null;
        Node myTempNode2 = null;

        try {
            myTempNode1 = findNodeByUserid(from + "");
            myTempNode2 = findNodeByUserid(to + "");

            myTempNode1.createRelationshipTo(myTempNode2, relType);
            tx.success();

        } finally {
            tx.close();
        }
    }

    public Node findNodeByUserid(String userid) {
        Node myTempNode = null;
        Label userLabel = DynamicLabel.label("user");
        ArrayList<Node> userNodes = new ArrayList<Node>();
        Transaction tx = graphDb.beginTx();
        ResourceIterator<Node> users;
        try {
            users = graphDb.findNodesByLabelAndProperty(userLabel, "userid", userid + "").iterator();
            userNodes = new ArrayList<Node>();
            while (users.hasNext()) {
                userNodes.add(users.next());
            }

            for (Node node : userNodes) {
                myTempNode = node;
            }
        } finally {

        }
        return myTempNode;

    }

    @Override
    public void createSchema(Properties props) {
        IndexDefinition indexDefinition;
        Transaction tx = graphDb.beginTx();
        try {
            Schema schema = graphDb.schema();
            indexDefinition = schema.indexFor(DynamicLabel.label("user"))
                    .on("userid")
                    .create();
            schema.awaitIndexOnline(indexDefinition, 10, TimeUnit.SECONDS);

            tx.success();
        } finally {
            tx.close();
        }

    }

    @Override
    public int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds) {
        queryFriendshipIDs(memberID, pendingIds, RelTypes.PENDING_FRIEND);
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private void queryFriendshipIDs(int memberid, Vector<Integer> ids, RelTypes type) {
        Transaction tx = graphDb.beginTx();
        try {
            Node myTempNode = findNodeByUserid(memberid + "");
            Iterable<Relationship> relationships = myTempNode.getRelationships(type, Direction.INCOMING);
            for (Relationship r : relationships) {
                ids.add(Integer.parseInt(r.getOtherNode(myTempNode).getProperty("userid").toString()));
            }
            tx.success();
        } finally {
            tx.close();
        }
    }

    @Override
    public int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds) {
        queryFriendshipIDs(memberID, confirmedIds, RelTypes.FRIEND);
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
