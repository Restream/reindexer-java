rx-connector
====================

Reindexer is an embeddable, in-memory, document-oriented database with a high-level Query builder interface. 
Rx-connector allows to connect to a Reindexer instance from java-application.

## Usage

Here is example of basic rx-connector usage:

```java
//Define an item class
public class Item {

    // 'id' is a primary key
    @Reindex(name = "id", isPrimaryKey = true)
    private Integer id;

    // add index by 'name' field
    @Reindex(name = "name")
    private String name;

    // add index articles by 'articles' array
    @Reindex(name = "articles")
    private List<Integer> articles;

    // add sortable index by 'year' field
    @Reindex(name = "year", type = TREE)
    private Integer year;

    @Override
    public String toString() {
        return "Item{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", articles=" + articles +
                ", year=" + year +
                '}';
    }

    public Item(Integer id, String name, List<Integer> articles, Integer year) {
        this.id = id;
        this.name = name;
        this.articles = articles;
        this.year = year;
    }

    public static void main(String[] args) throws Exception {

        // Init a database instance and choose the binding (builtin). Configure connection pool size and connection
        // timeout. Database should be created explicitly via reindexer_tool.
        Reindexer db = Configuration.builder()
                .url("cproto://localhost:6534/testdb")
                .connectionPoolSize(1)
                .requestTimeout(Duration.ofSeconds(30L))
                .getReindexer();

        // Create new namespace with name 'items', which will store objects of type 'Item'
        db.openNamespace("items", NamespaceOptions.defaultOptions(), Item.class);

        // Generate dataset
        for (int i = 0; i < 1000000; i++) {
            Random random = new Random();
            db.upsert("items", new Item(
                    i,
                    "Vasya",
                    Arrays.asList(random.nextInt() % 100, random.nextInt() % 100), 2000 + random.nextInt() % 50)
            );
        }

        // Query multiple documents, execute the query and return an iterator
        CloseableIterator<Item> iterator = db.query("items", Item.class)
                .sort("year", false)
                .where("name", EQ, "Vasya")
                .where("year", GT, 2020)
                .where("articles", SET, 6, 1, 8)
                .limit(10)
                .offset(0)
                .execute();

        // Iterate over results
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }

        // Iterator must be closed
        iterator.close();

        //Update single item
        db.query("items", Item.class)
            .where("id", EQ, 5)
            .set("name", "Vova")
            .update();
        
        //Update multiple fields
        db.query("items", Item.class)
            .where("id", EQ, 5)
            .set("name", "Vova")
            .set("year", 2021)
            .update();

        //Update multiple items items
        db.query("items", Item.class)
            .where("id", LT, 5)
            .set("name", "Petya")
            .update();
        
        //Drop an item field
        db.query("items", Item.class)
            .where("id", EQ, 6)
            .drop("name");

    }

}
```

An alternative way to perform queries is to use the "Namespace" object, which can be obtained by opening the namespace using the Reindexer.openNamespace method:
```java
Namespace itemNamespace = db.openNamespace("items", NamespaceOptions.defaultOptions(), Item.class);
Item item = namespace.query()
        .where("name", EQ, "Vasya")
        .getOne();
```
### Complex Primary Keys and Composite Indexes

A Document can have multiple fields as a primary key. To enable this feature add composite index to object.
Composite index is an index that involves multiple fields, it can be used instead of several separate indexes.

```java
// Composite index
@Reindex(name = "id+sub_id", isPrimaryKey = true, subIndexes = {"id", "sub_id"})
public class Item {

    // 'id' is a part of a primary key
    @Reindex(name = "id")
    private Integer id;

    // 'sub_id' is a part of a primary key
    @Reindex(name = "sub_id")
    private String subId;

}
```

Query for composite index:

```java
db.query("items", Item.class)
    .whereComposite("id+sub_id", EQ, 1, "test")
    .execute();
```

### Joins
Reindexer can join documents from multiple namespaces into a single result:

```java
    import ru.rt.restream.reindexer.annotations.Transient;

public class Actor {

    @Reindex(name = "id", isPrimaryKey = true)
    private Integer id;

    @Reindex(name = "name")
    private String name;

    @Reindex(name = "is_visible")
    private boolean visible;

}

public class ItemWithJoin {

    @Reindex(name = "id", isPrimaryKey = true)
    private Integer id;

    @Reindex(name = "name")
    private String name;

    private List<Integer> actorsIds;

    private String actorName;

    @Transient
    private List<Actor> joinedActors;

    @Transient
    private Actor joinedActor;
    
}
    
    
    //Select all items inner join actors on Actor.id in ItemWithJoin.actorIds
    CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
            .join(db.query("actors", Actor.class)
                    .on("actorsIds", Query.Condition.SET, "id"), "joinedActors")
            .execute();

    //Select all items inner join visible actors on Actor.id in ItemWithJoin.actorIds
    CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
            .join(db.query("actors", Actor.class).where("is_visible", EQ, true)
                    .on("actorsIds", Query.Condition.SET, "id"), "joinedActors")
            .execute();

    //Select all items inner join actors on Actor.name equal ItemWithJoin.actorName
    CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
            .join(db.query("actors", Actor.class)
                    .on("actorName", Query.Condition.EQ, "name"), "joinedActor")
            .execute();

```

Join query may have from one to several On conditions connected with **And** (by default), or **Or** operators:

```java
    CloseableIterator<ItemWithJoin> items = db.query("items_with_join",ItemWithJoin.class)
        .join(db.query("actors", Actor.class)
        .on("actorsIds",Query.Condition.SET,"id")
        .on("actorName",Query.Condition.SET,"name"),"joinedActors")
        .execute();
```

An InnerJoin combines data from two namespaces where there is a match on the joining fields in both namespaces. A LeftJoin returns all valid items from the namespaces on the left side of the LeftJoin keyword, along with the values from the table on the right side, or nothing if a matching item doesn't exist.  
InnerJoins can be used as a condition in Where clause:
```java
        Query<ItemWithJoin> query1 = db.query("items_with_join", ItemWithJoin.class)
                .where("id", RANGE, 0, 100)
                .or()
                .innerJoin(db.query("actors", Actor.class)
                        .where("name", EQ, "Test")
                        .on("actorsIds", SET, "id"), "joinedActors")
                .or()
                .innerJoin(db.query("actors", Actor.class)
                        .where("id", RANGE, 200, 300)
                        .on("actorsIds", SET, "id"), "joinedActors")
                .execute();  

        Query<ItemWithJoin> query2 = db.query("items_with_join", ItemWithJoin.class)
                .where("id", RANGE, 0, 100)
                .or()
                .openBracket()
                .innerJoin(db.query("actors", Actor.class)
                        .where("name", EQ, "Test")
                        .on("actorsIds", SET, "id"), "joinedActors")
                .innerJoin(db.query("actors", Actor.class)
                        .where("id", RANGE, 200, 300)
                        .on("actorsIds", SET, "id"), "joinedActors")
                .closeBracket();
        
        Query<ItemWithJoin> query3 = db.query("items_with_join", ItemWithJoin.class)
                .where("id", RANGE, 0, 100)
                .or()
                .innerJoin(db.query("actors", Actor.class)
                        .where("id", RANGE, 200, 300)
                        .on("actorsIds", SET, "id")
                        .limit(0), "joinedActors");
```
Note that usually Or operator implements short-circuiting for Where conditions: if the previous condition is true the next one is not evaluated. But in case of InnerJoin it works differently: in query1 (from the example above) both InnerJoin conditions are evaluated despite the result of WhereInt. Limit(0) as part of InnerJoin (query3 from the example above) does not join any data - it works like a filter only to verify conditions.

### Transactions and batch update

Reindexer supports transactions. Transaction are performs atomic namespace update. There are synchronous and 
async transaction available. To start transaction method db.beginTransaction() is used. This method creates transaction 
object, which provides usual Update/Upsert/Insert/Delete interface for application. For RPC clients there is 
transactions count limitation - each connection can't has more than 1024 opened transactions at the same time.

#### Synchronous mode

```java
// Create new transaction object
Transaction<Item> tx = db.beginTransaction("items", Item.class);
// Fill transaction object
tx.upsert(new Item(100, "Vasya", Arrays.asList(6, 1, 8), 2019));
tx.upsert(new Item(101, "Vova", Arrays.asList(7, 2, 9), 2020));
tx.query().where("id", EQ, 102).set("name", "Petya").update();
// Apply transaction
tx.commit();
```

#### Async batch mode

```java
// Create new transaction object
Transaction<Item> tx = db.beginTransaction("items", Item.class);
// Prepare transaction object async
tx.upsertAsync(new Item(100, "Vasya", Arrays.asList(6, 1, 8), 2019));
tx.upsertAsync(new Item(101, "Vova", Arrays.asList(7, 2, 9), 2020))
        .thenAccept(item -> processItem(item))
        .exceptionally(e -> handleError(e));
// Wait for async operations done, and apply transaction
tx.commit();
```

The return value of `tx.upsertAsync` is `CompletableFuture`, which will be completed after receiving server response. 
Also, if any error occurred during prepare process, then `tx.commit` should return an error. So it is enough, to 
check error returned by `tx.commit` - to be sure, that all data has been successfully committed or not.

#### Transactions commit strategies

Depends on amount changes in transaction there are 2 possible Commit strategies:

- Locked atomic update. Reindexer locks namespace and applying all changes under common lock. This mode is used with small amounts of changes.
- Copy & atomic replace. In this mode Reindexer makes namespace's snapshot, applying all changes to this snapshot, and atomically replaces namespace without lock.

#### Implementation notes

1. Transaction object is not thread safe and can't be used from different threads.
2. Transaction object holds Reindexer's resources, therefore application should explicitly call `tx.rollback` or `tx.commit`, otherwise resources will leak.
3. It is safe to call `tx.rollback` after `tx.commit`.
4. It is possible to call Query from transaction by call `tx.query().execute(); ...`. Only read-committed isolation is available. Changes made in active transaction is invisible to current and another transactions.
