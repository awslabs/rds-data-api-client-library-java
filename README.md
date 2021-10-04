# RDS Data API Client Library for Java

The **RDS Data API Client Library for Java** provides an alternative way 
to use RDS Data API. Using this library, you can map your client-side 
classes to requests and responses of the Data API. This mapping support 
can ease integration with some specific Java types, such as `Date`, `Time`, 
and `BigDecimal`.

### Getting the Java Client Library for RDS Data API
The Data API Java client library is open source in GitHub. You can build 
the library manually from the source files, but the best practice is to 
consume the library using Maven: 

```xml
<dependency>
   <groupId>software.amazon.rdsdata</groupId>
   <artifactId>rds-data-api-client-library-java</artifactId>
   <version>1.0.7</version>
</dependency>
```

### Using the Client Library
Following, you can find some common examples of using the Data API Java client library. These examples assume that you have a table accounts with two columns: accountId and name. You also have the following data transfer object (DTO).

```java
public class Account {
    int accountId;
    String name;
    // getters and setters omitted
}  
```                               
                
The client library enables you to pass DTOs as input parameters. The following example shows how customer DTOs are mapped to input parameters sets.

```java
var account1 = new Account(1, "John");
var account2 = new Account(2, "Mary");
client.forSql("INSERT INTO accounts(accountId, name) VALUES(:accountId, :name)")
         .withParamSets(account1, account2)
         .execute();   
```             
                
In some cases, it's easier to work with simple values as input parameters. You can do so with the following syntax.

```java
client.forSql("INSERT INTO accounts(accountId, name) VALUES(:accountId, :name)")
         .withParameter("accountId", 3)
         .withParameter("name", "Karen")
         .execute();  
```            
                
The following is another example that works with simple values as input parameters.

```java
client.forSql("INSERT INTO accounts(accountId, name) VALUES(?, ?)", 4, "Peter")
         .execute();    
```        
                
The client library provides automatic mapping to DTOs when an execution result is returned. The following examples show how the execution result is mapped to your DTOs.

```java
List<Account> result = client.forSql("SELECT * FROM accounts")
          .execute()
          .mapToList(Account.class);
```

```java
Account result = client.forSql("SELECT * FROM accounts WHERE account_id = 1")
          .execute()
          .mapToSingle(Account.class);          
```                

In many cases, the database result set contains only a single value. In order to simplify retrieving such results, the client library offers the following API:

```java
int numberOfAccounts = client.forSql("SELECT COUNT(*) FROM accounts")
          .execute()
          .singleValue(Integer.class);          
``` 